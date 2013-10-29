#!/usr/bin/env python

import os
import sys
import time

from subprocess import Popen

import pika

BUFSIZE = 4096
RMQHOST = os.environ.get('STREAMBOSS_RABBITMQ_HOST', 'localhost')
RABBITMQ_USER = os.environ.get('STREAMBOSS_RABBITMQ_USER', 'guest')
RABBITMQ_PASSWORD = os.environ.get('STREAMBOSS_RABBITMQ_PASSWORD', 'guest')


class TestStreamAgent(object):

    def __init__(self):
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(RMQHOST, credentials=credentials))
        self.channel = self.connection.channel()

        self.input_stream = 'instream'
        self.output_stream = 'outstream'
        self.exchange_name = 'streams'

        self.exchange = self.channel.exchange_declare(exchange=self.exchange_name, auto_delete=True)

        self.input_queue = self.channel.queue_declare(queue=self.input_stream)
        self.channel.queue_bind(exchange=self.exchange_name,
                queue=self.input_queue.method.queue, routing_key=self.input_stream)

        self.output_queue = self.channel.queue_declare(queue=self.output_stream)
        self.channel.queue_bind(exchange=self.exchange_name,
                queue=self.output_queue.method.queue, routing_key=self.output_stream)

    def consume_func(self, ch, method, properties, body):
        if method.exchange == self.exchange_name:
            message = body

            assert message == "TEST\n", "message is '%s' expected '%s'" % (message, "TEST")
            sys.exit(0)

    def start(self):
        body = "test"
        self.channel.basic_publish(exchange='streams', routing_key=self.input_stream, body=body)
        self.channel.basic_consume(self.consume_func, queue=self.output_queue.method.queue, no_ack=True)

        self.p = Popen(['python', 'stream_agent.py', '{"exec": "tr \'[a-z]\' \'[A-Z]\'"}',
            self.input_stream, self.output_stream])
        self.connection.close()

        time.sleep(4)
        self.p.kill()
        sys.exit("TEST FAILED, had to kill agent")


if __name__ == '__main__':
    TestStreamAgent().start()
