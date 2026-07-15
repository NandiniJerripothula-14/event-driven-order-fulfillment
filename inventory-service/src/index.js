const express = require('express');
const { Pool } = require('pg');
const amqp = require('amqplib');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3002;
const DB_NAME = process.env.DB_NAME || 'inventory_db';
const DB_USER = process.env.DB_USER || 'postgres';
const DB_PASSWORD = process.env.DB_PASSWORD || 'postgres';
const DB_HOST = process.env.DB_HOST || 'postgres';
const DB_PORT = parseInt(process.env.DB_PORT || '5432');
const BROKER_URL = process.env.BROKER_URL || 'amqp://guest:guest@rabbitmq:5672';

const pool = new Pool({
  host: DB_HOST,
  user: DB_USER,
  password: DB_PASSWORD,
  database: DB_NAME,
  port: DB_PORT,
});

let channel;
let isReady = false;

async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS processed_events (
      event_id VARCHAR PRIMARY KEY,
      processed_at TIMESTAMP DEFAULT NOW()
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS inventory (
      product_id VARCHAR PRIMARY KEY,
      stock INT NOT NULL
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS reservations (
      id UUID PRIMARY KEY,
      order_id UUID NOT NULL,
      product_id VARCHAR NOT NULL,
      quantity INT NOT NULL,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);

  // Seed inventory
  const result = await pool.query('SELECT COUNT(*) FROM inventory');
  if (parseInt(result.rows[0].count) === 0) {
    await pool.query("INSERT INTO inventory (product_id, stock) VALUES ('prod-123', 100), ('FAIL-ME', 100)");
    console.log('Seeded database with initial items.');
  }

  console.log('Database initialized successfully.');
}

async function connectRabbitMQ() {
  const maxRetries = 10;
  let retries = 0;
  let connection;

  while (retries < maxRetries) {
    try {
      connection = await amqp.connect(BROKER_URL);
      break;
    } catch (err) {
      retries++;
      console.warn(`Failed to connect to RabbitMQ (attempt ${retries}/${maxRetries}): ${err.message}`);
      if (retries >= maxRetries) throw err;
      await new Promise(resolve => setTimeout(resolve, 3000));
    }
  }

  connection.on('close', () => {
    console.error('RabbitMQ connection closed. Exiting...');
    process.exit(1);
  });
  connection.on('error', (err) => {
    console.error('RabbitMQ connection error. Exiting...', err);
    process.exit(1);
  });

  channel = await connection.createChannel();

  await channel.assertExchange('order_exchange', 'topic', { durable: true });
  await channel.assertExchange('dlx_exchange', 'topic', { durable: true });

  // Assert DLQ queue and bind it to DLX exchange
  await channel.assertQueue('dlq_queue', { durable: true });
  await channel.bindQueue('dlq_queue', 'dlx_exchange', '#');

  const queueName = 'payment_processed_queue';
  await channel.assertQueue(queueName, {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': 'dlx_exchange',
      'x-dead-letter-routing-key': 'dlq.payment_processed'
    }
  });

  await channel.bindQueue(queueName, 'order_exchange', 'payment.processed');

  channel.consume(queueName, async (msg) => {
    if (!msg) return;

    let event;
    try {
      event = JSON.parse(msg.content.toString());
    } catch (e) {
      console.error('Failed to parse JSON content from message.', e);
      channel.nack(msg, false, false); // Nack directly to DLQ
      return;
    }

    const headers = msg.properties.headers || {};
    const retryCount = headers['x-retry-count'] || 0;

    const client = await pool.connect();
    try {
      console.log(`Processing PaymentProcessed event: ${event.eventId} for Order: ${event.aggregateId}. Retry Count: ${retryCount}`);

      // Hardcoded fail trigger for DLQ verification
      const hasFailMe = event.payload.items && event.payload.items.some(item => item.productId === 'FAIL-ME');
      if (hasFailMe) {
        throw new Error('Hardcoded failure triggered: product is FAIL-ME');
      }

      await client.query('BEGIN');

      // 1. Check Idempotency
      const dupCheck = await client.query('SELECT 1 FROM processed_events WHERE event_id = $1', [event.eventId]);
      if (dupCheck.rows.length > 0) {
        console.log(`Event ${event.eventId} already processed. Skipping business logic.`);
        await client.query('ROLLBACK');
        channel.ack(msg);
        return;
      }

      // 2. Reserve items
      for (const item of event.payload.items) {
        const { productId, quantity } = item;

        // Deduct inventory
        const res = await client.query(
          'UPDATE inventory SET stock = stock - $1 WHERE product_id = $2 AND stock >= $3 RETURNING stock',
          [quantity, productId, quantity]
        );

        if (res.rows.length === 0) {
          throw new Error(`Insufficient inventory for product: ${productId}`);
        }

        // Insert reservation record
        const reservationId = uuidv4();
        await client.query(
          'INSERT INTO reservations (id, order_id, product_id, quantity) VALUES ($1, $2, $3, $4)',
          [reservationId, event.aggregateId, productId, quantity]
        );
      }

      // 3. Mark event as processed
      await client.query('INSERT INTO processed_events (event_id) VALUES ($1)', [event.eventId]);

      await client.query('COMMIT');

      // 4. Publish next event
      const reservedEvent = {
        eventId: uuidv4(),
        eventType: 'InventoryReserved',
        timestamp: new Date().toISOString(),
        aggregateId: event.aggregateId,
        payload: {
          items: event.payload.items,
          totalPrice: event.payload.totalPrice
        }
      };

      channel.publish(
        'order_exchange',
        'inventory.reserved',
        Buffer.from(JSON.stringify(reservedEvent)),
        { persistent: true }
      );

      console.log(`Published InventoryReserved event for Order: ${event.aggregateId}`);
      channel.ack(msg);
    } catch (err) {
      await client.query('ROLLBACK');
      console.error(`Error processing event ${event ? event.eventId : 'unknown'}:`, err.message);

      if (retryCount >= 2) {
        console.log(`Max retries reached for event ${event ? event.eventId : 'unknown'}. Routing to DLQ.`);
        // Nack to DLQ (requeue: false)
        channel.nack(msg, false, false);
      } else {
        const nextRetry = retryCount + 1;
        console.log(`Re-queueing message with incremented retry count: ${nextRetry}`);
        // Publish back to the same queue with incremented retry count
        const updatedHeaders = { ...headers, 'x-retry-count': nextRetry };
        channel.sendToQueue(queueName, msg.content, { headers: updatedHeaders, persistent: true });
        channel.ack(msg);
      }
    } finally {
      client.release();
    }
  });

  console.log('Connected to RabbitMQ and consuming PaymentProcessed events.');
}

app.get('/health', (req, res) => {
  if (isReady) {
    return res.status(200).send('OK');
  }
  return res.status(503).send('Not Ready');
});

async function start() {
  try {
    await initDb();
    await connectRabbitMQ();
    isReady = true;
    app.listen(PORT, () => {
      console.log(`Inventory Service running on port ${PORT}`);
    });
  } catch (err) {
    console.error('Startup failed:', err);
    process.exit(1);
  }
}

start();
