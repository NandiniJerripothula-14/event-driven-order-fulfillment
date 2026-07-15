const express = require('express');
const { Pool } = require('pg');
const amqp = require('amqplib');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const DB_NAME = process.env.DB_NAME || 'order_db';
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
  const query = `
    CREATE TABLE IF NOT EXISTS orders (
      id UUID PRIMARY KEY,
      items JSONB NOT NULL,
      total_price DECIMAL(10, 2) NOT NULL,
      status VARCHAR(50) NOT NULL,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `;
  await pool.query(query);
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

  // Declare exchanges
  await channel.assertExchange('order_exchange', 'topic', { durable: true });
  await channel.assertExchange('dlx_exchange', 'topic', { durable: true });

  // Setup order status update queue
  const queueName = 'order_status_queue';
  await channel.assertQueue(queueName, { durable: true });
  
  // Bind order status queue to receive events that update the order state
  await channel.bindQueue(queueName, 'order_exchange', 'payment.processed');
  await channel.bindQueue(queueName, 'order_exchange', 'inventory.reserved');
  await channel.bindQueue(queueName, 'order_exchange', 'shipment.created');

  channel.consume(queueName, async (msg) => {
    if (!msg) return;

    try {
      const event = JSON.parse(msg.content.toString());
      console.log(`Received status update event: ${event.eventType} for Order: ${event.aggregateId}`);

      let newStatus;
      if (event.eventType === 'PaymentProcessed') {
        newStatus = 'PAID';
      } else if (event.eventType === 'InventoryReserved') {
        newStatus = 'INVENTORY_RESERVED';
      } else if (event.eventType === 'ShipmentCreated') {
        newStatus = 'SHIPPED';
      }

      if (newStatus) {
        await pool.query(
          'UPDATE orders SET status = $1 WHERE id = $2',
          [newStatus, event.aggregateId]
        );
        console.log(`Updated Order ${event.aggregateId} status to ${newStatus}`);
      }

      channel.ack(msg);
    } catch (err) {
      console.error('Error processing status update event:', err);
      // Nack status update events back to queue (with requeue)
      channel.nack(msg, false, true);
    }
  });

  console.log('Connected to RabbitMQ and consuming status updates.');
}

// Healthcheck endpoint
app.get('/health', (req, res) => {
  if (isReady) {
    return res.status(200).send('OK');
  }
  return res.status(503).send('Not Ready');
});

// POST /api/orders
app.post('/api/orders', async (req, res) => {
  const { items, totalPrice } = req.body;

  if (!items || !Array.isArray(items) || items.length === 0 || totalPrice === undefined) {
    return res.status(400).json({ error: 'Invalid request body' });
  }

  const orderId = uuidv4();

  try {
    // 1. Insert order to DB (Status: PENDING)
    await pool.query(
      'INSERT INTO orders (id, items, total_price, status) VALUES ($1, $2, $3, $4)',
      [orderId, JSON.stringify(items), totalPrice, 'PENDING']
    );

    // 2. Build Event Envelope
    const event = {
      eventId: uuidv4(),
      eventType: 'OrderCreated',
      timestamp: new Date().toISOString(),
      aggregateId: orderId,
      payload: {
        totalPrice,
        items
      }
    };

    // 3. Publish to Broker
    channel.publish(
      'order_exchange',
      'order.created',
      Buffer.from(JSON.stringify(event)),
      { persistent: true }
    );

    console.log(`Published OrderCreated event for Order: ${orderId}`);

    return res.status(202).json({ orderId });
  } catch (err) {
    console.error('Failed to create order:', err);
    return res.status(500).json({ error: 'Internal Server Error' });
  }
});

// GET /api/orders/:orderId
app.get('/api/orders/:orderId', async (req, res) => {
  const { orderId } = req.params;

  try {
    const result = await pool.query('SELECT * FROM orders WHERE id = $1', [orderId]);

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Order not found' });
    }

    const order = result.rows[0];
    return res.status(200).json({
      orderId: order.id,
      status: order.status,
      items: order.items,
      totalPrice: parseFloat(order.total_price),
      createdAt: order.created_at
    });
  } catch (err) {
    console.error('Failed to get order status:', err);
    return res.status(500).json({ error: 'Internal Server Error' });
  }
});

async function start() {
  try {
    await initDb();
    await connectRabbitMQ();
    isReady = true;
    app.listen(PORT, () => {
      console.log(`Order Service running on port ${PORT}`);
    });
  } catch (err) {
    console.error('Startup failed:', err);
    process.exit(1);
  }
}

start();
