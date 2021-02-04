from kombu import Connection, Exchange, Queue

media_exchange = Exchange('media', 'direct', durable=True)
video_queue = Queue('video', exchange=media_exchange, routing_key='video')

print("media_exchange: ", media_exchange)
print("video_queue: ", video_queue)


def process_media(body, message):
    print(body)
    message.ack()


# connections
with Connection("redis://localhost:6379/") as conn:

    # produce
    producer = conn.Producer(serializer='json')
    print("producer: ", producer)
    task = producer.publish({'name': 'vipin_soni', 'size': 1301013},
                      exchange=media_exchange, routing_key='video',
                      declare=[video_queue])
    print("task: ", task)
    # the declare above, makes sure the video queue is declared
    # so that the messages can be delivered.
    # It's a best practice in Kombu to have both publishers and
    # consumers declare the queue. You can also declare the
    # queue manually using:
    #     video_queue(conn).declare()

    # consume
    with conn.Consumer(video_queue, callbacks=[process_media]) as consumer:
        # Process messages and handle events on all channels
        while True:
            conn.drain_events()
