#!/usr/bin/env python

import os
import sys
import pika
import threading

from subprocess import Popen, PIPE

from stream_boss import EXCHANGE_PREFIX


BUFSIZE = 4096
RMQHOST = os.environ.get('STREAMBOSS_RABBITMQ_HOST', 'localhost')
RABBITMQ_USER = os.environ.get('STREAMBOSS_RABBITMQ_USER', 'guest')
RABBITMQ_PASSWORD = os.environ.get('STREAMBOSS_RABBITMQ_PASSWORD', 'guest')


class TestStreamAgent(object):

    def __init__(self):
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(RMQHOST, credentials=credentials))
        self.channel = self.connection.channel()

        start_agent = int(os.environ.get("START_AGENT", 1))
        self.start_agent = True if start_agent == 1 else False

        self.input_stream = os.environ.get('INPUT_STREAM', 'instream')
        self.output_stream = os.environ.get('OUTPUT_STREAM', 'outstream')
        self.input_exchange_name = "%s.%s" % (EXCHANGE_PREFIX, self.input_stream)
        self.output_exchange_name = "%s.%s" % (EXCHANGE_PREFIX, self.output_stream)

        self.input_exchange = self.channel.exchange_declare(exchange=self.input_exchange_name, type='fanout')
        self.output_exchange = self.channel.exchange_declare(exchange=self.output_exchange_name, type='fanout')

        self.output_queue = self.channel.queue_declare(auto_delete=True)
        self.channel.queue_bind(exchange=self.output_exchange_name,
                queue=self.output_queue.method.queue)

    def consume_func(self, ch, method, properties, body):
        if method.exchange == self.output_exchange_name:
            message = body

            assert message == "TEST", "message is '%s' expected '%s'" % (message, "TEST")
            print "TEST OK"
            self.cleanup()
        else:
            print "Message from unrecognized exchange"

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
        self.channel.basic_consume(self.consume_func, queue=self.output_queue.method.queue)

        if self.start_agent:
            self.p = Popen(['python', 'stream_agent.py', '{"exec": "tr \'[a-z]\' \'[A-Z]\'"}',
                self.input_stream, self.output_stream], stdout=PIPE, stderr=PIPE)
        else:
            self.p = None

        t = threading.Timer(
            1, self.channel.basic_publish,
            kwargs=dict(exchange=self.input_exchange_name, routing_key='', body=body)
        )
        t.start()
        self.connection.add_timeout(9, self.timeout)

        self.channel.start_consuming()

if __name__ == '__main__':
    TestStreamAgent().start()
