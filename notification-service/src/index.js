const express = require('express');
const amqp = require('amqplib');

const app = express();
const PORT = process.env.PORT || 3004;
const BROKER_URL = process.env.BROKER_URL || 'amqp://guest:guest@rabbitmq:5672';

let channel;
let isReady = false;

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

  const queueName = 'notification_queue';
  await channel.assertQueue(queueName, { durable: true });

  await channel.bindQueue(queueName, 'order_exchange', 'order.created');
  await channel.bindQueue(queueName, 'order_exchange', 'payment.processed');
  await channel.bindQueue(queueName, 'order_exchange', 'inventory.reserved');
  await channel.bindQueue(queueName, 'order_exchange', 'shipment.created');

  channel.consume(queueName, (msg) => {
    if (!msg) return;

    try {
      const event = JSON.parse(msg.content.toString());
      console.log(`Notification sent for event: ${event.eventType}, Order ID: ${event.aggregateId}`);
      channel.ack(msg);
    } catch (err) {
      console.error('Failed to process notification event:', err);
      // Nack notification event back to queue (with requeue)
      channel.nack(msg, false, true);
    }
  });

  console.log('Connected to RabbitMQ and consuming notification events.');
}

app.get('/health', (req, res) => {
  if (isReady) {
    return res.status(200).send('OK');
  }
  return res.status(503).send('Not Ready');
});

async function start() {
  try {
    await connectRabbitMQ();
    isReady = true;
    app.listen(PORT, () => {
      console.log(`Notification Service running on port ${PORT}`);
    });
  } catch (err) {
    console.error('Startup failed:', err);
    process.exit(1);
  }
}

start();
