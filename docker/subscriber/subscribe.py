#!/usr/bin/env python

# Listen on the example RabbitMQ fanout exchange and print its messages

import ConfigParser, logging, json, os, sys, time
import pika

logging.basicConfig(
    format='%(message)s',
    level=getattr(logging, 'INFO', 10),
)

config_path = os.path.realpath(sys.path[0] + '/')
config = ConfigParser.RawConfigParser()
file_path = config_path + '/config.cfg'
config.read(file_path)

# Give the RabbitMQ docker a few seconds to start
time.sleep(5)

parameters = pika.ConnectionParameters(
    host=config.get('amqp', 'host'),
    port=config.get('amqp', 'port'),
    ssl=False,
    virtual_host=config.get('amqp', 'vhost'),
    credentials=pika.PlainCredentials(
        config.get('amqp', 'user'), config.get('amqp', 'password')
    ),
    connection_attempts=5,
    retry_delay=5
)
connection = pika.BlockingConnection(parameters)
channel = connection.channel(1)
channel.exchange_declare(
    exchange=config.get('amqp', 'exchange'),
    exchange_type=config.get('amqp', 'exchange_type'),
    passive=False,
    durable=True,
    auto_delete=False
)

result = channel.queue_declare()
queue_name = result.method.queue
channel.queue_bind(exchange=config.get('amqp', 'exchange'), queue=queue_name)


def consumer(channel, method, properties, body):
    body_json = json.loads(body)
    logging.info(json.dumps(body_json, indent=4))

try:
    channel.basic_consume(consumer, queue=queue_name, no_ack=True)
    channel.start_consuming()
except KeyboardInterrupt:
    channel.queue_delete(queue=queue_name)
    connection.close()
exit(0)

