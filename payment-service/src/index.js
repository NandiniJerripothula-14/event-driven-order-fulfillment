const express = require('express');
const { Pool } = require('pg');
const amqp = require('amqplib');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3001;
const DB_NAME = process.env.DB_NAME || 'payment_db';
const DB_USER = process.env.DB_USER || 'postgres';
const DB_PASSWORD = process.env.DB_PASSWORD || 'postgres';
const DB_HOST = process.env.DB_HOST || 'postgres';
const DB_PORT = parseInt(process.env.DB_PORT || '5432');
const BROKER_URL = process.env.BROKER_URL || 'amqp://guest:guest@rabbitmq:5672';

// DB Pool
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
    CREATE TABLE IF NOT EXISTS payments (
      id UUID PRIMARY KEY,
      order_id UUID NOT NULL,
      amount DECIMAL(10, 2) NOT NULL,
      status VARCHAR(50) NOT NULL,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);
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

  const queueName = 'order_created_queue';
  await channel.assertQueue(queueName, {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': 'dlx_exchange',
      'x-dead-letter-routing-key': 'dlq.order_created'
    }
  });

  await channel.bindQueue(queueName, 'order_exchange', 'order.created');

  channel.consume(queueName, async (msg) => {
    if (!msg) return;

    const client = await pool.connect();
    try {
      const event = JSON.parse(msg.content.toString());
      console.log(`Processing OrderCreated event: ${event.eventId} for Order: ${event.aggregateId}`);

      await client.query('BEGIN');

      // 1. Check Idempotency
      const dupCheck = await client.query('SELECT 1 FROM processed_events WHERE event_id = $1', [event.eventId]);
      if (dupCheck.rows.length > 0) {
        console.log(`Event ${event.eventId} already processed. Skipping business logic.`);
        await client.query('ROLLBACK');
        channel.ack(msg);
        return;
      }

      // 2. Execute Business Logic (Simulated Payment)
      const paymentId = uuidv4();
      const status = event.payload.totalPrice > 0 ? 'SUCCESS' : 'FAILED';
      await client.query(
        'INSERT INTO payments (id, order_id, amount, status) VALUES ($1, $2, $3, $4)',
        [paymentId, event.aggregateId, event.payload.totalPrice, status]
      );

      // 3. Mark event as processed (inside the transaction)
      await client.query('INSERT INTO processed_events (event_id) VALUES ($1)', [event.eventId]);

      await client.query('COMMIT');

      // 4. Publish next event
      const processedEvent = {
        eventId: uuidv4(),
        eventType: 'PaymentProcessed',
        timestamp: new Date().toISOString(),
        aggregateId: event.aggregateId,
        payload: {
          paymentId,
          status,
          totalPrice: event.payload.totalPrice,
          items: event.payload.items
        }
      };

      channel.publish(
        'order_exchange',
        'payment.processed',
        Buffer.from(JSON.stringify(processedEvent)),
        { persistent: true }
      );

      console.log(`Published PaymentProcessed event for Order: ${event.aggregateId}`);
      channel.ack(msg);
    } catch (err) {
      await client.query('ROLLBACK');
      console.error('Failed to process payment event:', err);
      // Nack without requeue if we want it to go to DLQ on failure, or with requeue if temporary error
      // Let's nack to DLQ on parsing/DB issues (requeue = false)
      channel.nack(msg, false, false);
    } finally {
      client.release();
    }
  });

  console.log('Connected to RabbitMQ and consuming OrderCreated events.');
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
      console.log(`Payment Service running on port ${PORT}`);
    });
  } catch (err) {
    console.error('Startup failed:', err);
    process.exit(1);
  }
}

start();
