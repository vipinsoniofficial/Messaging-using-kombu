from kombu import Connection, Exchange, Producer, Queue, Consumer
rabbit_url = "redis://localhost:6379/"
print("rabbit_url: ", rabbit_url)
conn = Connection(rabbit_url)
print("conn: ", conn)

channel = conn.channel()
print("channel: ", channel)

exchange = Exchange("example-exchange", type="direct")
print("exchange: ", exchange)

producer = Producer(exchange=exchange, channel=channel, routing_key='BOB')
print("producer: ", producer)

queue = Queue(name="example-queue", exchange=exchange, routing_key='BOB')
print("queue: ", queue, "\n", queue.maybe_bind(conn), queue.declare())
queue.maybe_bind(conn)
queue.declare()

producer.publish("Hello there")
print("msg: ", producer.publish("Hello there"))

def process_body(body, message):
    print("Message: ", body)
    message.ack()


with Consumer(conn, queues=queue, callbacks=[process_body], accept=["text/plain"]):
    print("consumer: ", conn)
    conn.drain_events(timeout=2)