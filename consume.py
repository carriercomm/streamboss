#!/usr/bin/env python

import os
import sys
import time

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

        self.consume_stream = sys.argv[1]
        self.exchange_name = 'streams'

        self.exchange = self.channel.exchange_declare(exchange=self.exchange_name, auto_delete=True)

        self.consume_queue = self.channel.queue_declare(queue=self.consume_stream)
        self.channel.queue_bind(exchange=self.exchange_name,
                queue=self.consume_queue.method.queue, routing_key=self.consume_stream)

    def consume_func(self, ch, method, properties, body):
        if method.exchange == self.exchange_name:
            message = body
            print "GOT: '%s'" % message

    def start(self):
        print "Consuming..."
        try:
            self.channel.basic_consume(self.consume_func, queue=self.consume_queue.method.queue, no_ack=True)
        except Exception:
            self.connection.close()

        while True:
            time.sleep(1)

if __name__ == '__main__':
    TestStreamAgent().start()
