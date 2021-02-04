from kombu.pools import producers

from queuess import task_exchange

priority_to_routing_key = {
    'high': 'hipri',
    'mid': 'midpri',
    'low': 'lopri',
}


def send_as_task(connection, fun, args=(), kwargs={}, priority='mid'):
    payload = {'fun': fun, 'args': args, 'kwargs': kwargs}
    print("payload: ", payload)
    routing_key = priority_to_routing_key[priority]
    print("routing_key: ", routing_key)

    with producers[connection].acquire(block=True) as producer:
        producer.publish(payload,
                         serializer='pickle',
                         compression='bzip2',
                         exchange=task_exchange,
                         declare=[task_exchange],
                         routing_key=routing_key)


if __name__ == '__main__':
    from kombu import Connection
    from task import hello_task

    # connection = Connection('amqp://guest:guest@localhost:5672//')
    connection = Connection("redis://localhost:6379/")
    send_as_task(connection, fun=hello_task, args=('Kombus',), kwargs={},
                 priority='high')

