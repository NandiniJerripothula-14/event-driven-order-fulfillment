# Event-Driven E-Commerce Order Fulfillment System

A containerized, event-driven order fulfillment pipeline utilizing a Publish-Subscribe (Pub/Sub) pattern with RabbitMQ, isolated PostgreSQL databases, and five Node.js-based microservices.

## Tech Stack
- **Languages**: Node.js (JavaScript, Express)
- **Message Broker**: RabbitMQ (durable queues, exchanges, and dead letter routing)
- **Database**: PostgreSQL (isolated databases for Order, Payment, Inventory, and Shipping services)
- **Orchestration**: Docker & Docker Compose

## Architecture & Event Flow
The system processes orders asynchronously through a chain of message publications and consumptions:

1. **Order Service** (`POST /api/orders`) -> saves order as `PENDING` -> publishes `OrderCreated`.
2. **Payment Service** consumes `OrderCreated` -> processes simulated payment -> publishes `PaymentProcessed`.
3. **Inventory Service** consumes `PaymentProcessed` -> reserves items/stock -> publishes `InventoryReserved`.
4. **Shipping Service** consumes `InventoryReserved` -> generates tracking info -> publishes `ShipmentCreated`.
5. **Notification Service** consumes all events and logs alert details to stdout.
6. **Eventual Consistency**: The **Order Service** listens to all status update events (`PaymentProcessed`, `InventoryReserved`, `ShipmentCreated`) and updates the order status in its database to `PAID`, `INVENTORY_RESERVED`, and ultimately `SHIPPED`.

```
[Client] -> POST /api/orders -> [Order Service]
                                      |
                             (Publish: OrderCreated)
                                      |
                                      v
                               [Payment Service] -> (Publish: PaymentProcessed)
                                      |
                                      v
                              [Inventory Service] -> (Publish: InventoryReserved)
                                      |
                                      v
                              [Shipping Service] -> (Publish: ShipmentCreated)
                                      |
                                      v
[Client] <- GET /api/orders <- [Order Service] (Status updated eventually to SHIPPED)
```

## Setup & Running the System

### Prerequisites
- Docker & Docker Compose

### Start the System
To build and start all databases, RabbitMQ, and the microservices stack:
```bash
docker-compose up --build -d
```
All containers will run healthchecks and wait until their infrastructure dependencies (PostgreSQL and RabbitMQ) are fully ready.

---

## Verification & Testing

### 1. Place a Standard Order
```powershell
$response = Invoke-RestMethod -Uri "http://localhost:3000/api/orders" -Method Post -ContentType "application/json" -Body '{"items": [{"productId": "prod-123", "quantity": 2, "price": 10.50}], "totalPrice": 21.00}'
$orderId = $response.orderId
echo "Placed order: $orderId"
```

Check the status evolution (wait 5 seconds for background pipeline processing):
```powershell
$order = Invoke-RestMethod -Uri "http://localhost:3000/api/orders/$orderId" -Method Get
$order | ConvertTo-Json
```
The status field should show **`SHIPPED`**.

Check the Notification Service logs:
```bash
docker-compose logs notification-service
```

### 2. Message Durability Test
1. Create a new order via the POST API.
2. Immediately restart the message broker:
   ```bash
   docker-compose restart rabbitmq
   ```
3. Wait 15-20 seconds.
4. Retrieve the order status using the GET API. It will successfully show `SHIPPED`, demonstrating that the messages survived the broker crash/restart.

### 3. Idempotency Test
Duplicate events are checked against the service database's `processed_events` table within a transaction. To verify, a duplicate event with the same ID can be sent twice:
- The first processing reserves stock.
- The second duplicate request logs:
  `Event [eventId] already processed. Skipping business logic.`

### 4. Dead Letter Queue (DLQ) Test
The Inventory Service is configured to fail when a product ID of `'FAIL-ME'` is processed.
1. Place an order with `'FAIL-ME'`:
   ```powershell
   Invoke-RestMethod -Uri "http://localhost:3000/api/orders" -Method Post -ContentType "application/json" -Body '{"items": [{"productId": "FAIL-ME", "quantity": 1, "price": 10.00}], "totalPrice": 10.00}'
   ```
2. The service retries processing 3 times.
3. Upon the 3rd failure, the message is routed to the DLQ (`dlq_queue`).
4. To check the DLQ contents, query RabbitMQ's management API:
   ```powershell
   $headers = @{Authorization = "Basic " + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("guest:guest"))}
   $body = '{"count": 5, "ackmode": "ack_requeue_true", "encoding": "auto"}'
   Invoke-RestMethod -Uri "http://localhost:15672/api/queues/%2F/dlq_queue/get" -Method Post -Headers $headers -ContentType "application/json" -Body $body
   ```
