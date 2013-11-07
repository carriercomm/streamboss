#!/usr/bin/env python

import os
import sys

from subprocess import Popen

import pika

BUFSIZE = 4096
RMQHOST = os.environ.get('STREAMBOSS_RABBITMQ_HOST', 'localhost')
RABBITMQ_USER = os.environ.get('STREAMBOSS_RABBITMQ_USER', 'guest')
RABBITMQ_PASSWORD = os.environ.get('STREAMBOSS_RABBITMQ_PASSWORD', 'guest')
WHEREAMI = os.path.dirname(os.path.realpath(__file__))


class TestStreamAgent(object):

    def __init__(self):
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(RMQHOST, credentials=credentials))
        self.channel = self.connection.channel()

        start_agent = int(os.environ.get("START_AGENT", 1))
        self.start_agent = True if start_agent == 1 else False

        self.input_stream = os.environ.get('INPUT_STREAM', 'instream')
        self.output_stream = os.environ.get('OUTPUT_STREAM', 'outstream')
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

            assert message == "test\n", "message is '%s' expected '%s'" % (message, "test")
            print "TEST OK"
            self.cleanup()

    def timeout(self):
        print "TEST FAILED"
        self.cleanup()
        sys.exit(1)

    def cleanup(self):
        self.channel.stop_consuming()
        self.connection.close()
        if self.p:
            self.p.kill()

    def start(self):
        body = "test"
        self.channel.basic_consume(self.consume_func, queue=self.output_queue.method.queue, no_ack=True)

        if self.start_agent:
            self.p = Popen(['python', 'stream_agent.py',
                '{"exec": "bash %s/test-producer.sh", "process_type": "service"}' % WHEREAMI,
                self.input_stream, self.output_stream])
        else:
            self.p = None

        self.channel.basic_publish(exchange='streams', routing_key=self.input_stream, body=body)
        self.connection.add_timeout(9, self.timeout)

        self.channel.start_consuming()

if __name__ == '__main__':
    TestStreamAgent().start()
