#!/usr/bin/env python

import os
import sys

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

        self.publish_stream = sys.argv[1]
        self.body = sys.argv[2]
        self.exchange_name = 'streams'

        self.exchange = self.channel.exchange_declare(exchange=self.exchange_name, auto_delete=True)

        self.publish_queue = self.channel.queue_declare(queue=self.publish_stream)
        self.channel.queue_bind(exchange=self.exchange_name,
                queue=self.publish_queue.method.queue, routing_key=self.publish_stream)

    def cleanup(self):
        self.channel.stop_consuming()
        self.connection.close()

    def start(self):

        self.channel.basic_publish(exchange='streams', routing_key=self.publish_stream, body=self.body)

if __name__ == '__main__':
    TestStreamAgent().start()
