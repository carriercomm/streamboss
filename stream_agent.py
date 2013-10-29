#!/usr/bin/env python

from subprocess import PIPE, Popen, STDOUT
import json
import os
import sys

import pika

BUFSIZE = 4096
RMQHOST = os.environ.get('STREAMBOSS_RABBITMQ_HOST', 'localhost')
RABBITMQ_USER = os.environ.get('STREAMBOSS_RABBITMQ_USER', 'guest')
RABBITMQ_PASSWORD = os.environ.get('STREAMBOSS_RABBITMQ_PASSWORD', 'guest')


class ProcessDispatcherAgent(object):

    def __init__(self):
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(RMQHOST, credentials=credentials))
        self.channel = self.connection.channel()

        if len(sys.argv) != 4:
            sys.exit("usage: %s process_definition input_stream output_stream" % sys.argv[0])

        self.process_definition = None

        # JSON used to be loaded from a file. Instead we get it directly as a string from argv[1]
        #with open(sys.argv[1]) as f:
            #json_data = f.read()
            #self.process_definition = json.loads(json_data)

        json_data = sys.argv[1]
        print json_data
        self.process_definition = json.loads(json_data)

        if self.process_definition is not None:
            self.process_path = self.process_definition["exec"]

        self.input_stream = sys.argv[2]
        self.output_stream = sys.argv[3]

        self.exchange_name = 'streams'

        self.exchange = self.channel.exchange_declare(exchange=self.exchange_name, auto_delete=True)

        self.input_queue = self.channel.queue_declare(queue=self.input_stream)
        self.output_queue = self.channel.queue_declare(queue=self.output_stream)
        self.channel.queue_bind(exchange=self.exchange_name,
                queue=self.input_queue.method.queue, routing_key=self.input_stream)
        self.channel.queue_bind(exchange=self.exchange_name,
                queue=self.output_queue.method.queue, routing_key=self.output_stream)

    def consume_func(self, ch, method, properties, body):
        if method.exchange == "streams":
            message = body
            print "< got: %s" % message

        p = Popen(self.process_path, shell=True, bufsize=BUFSIZE,
                stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
        (child_stdin, child_stdout, child_stderr) = (p.stdin, p.stdout, p.stderr)
        child_stdin.write(message + "\n")
        child_stdin.flush()
        # TODO: differentiate between ongoing and one-shot
        child_stdin.close()
        returncode = p.wait()
        output = child_stdout.read()
        print "> sending: %s" % output
        if returncode == 0:
            self.channel.basic_publish(exchange='streams', routing_key=self.output_stream, body=output)
        else:
            print "Error running %s: returned %d" % (self.process_path, returncode)

    def start(self):
        self.channel.basic_consume(self.consume_func, queue=self.input_queue.method.queue, no_ack=True)
        try:
            self.channel.start_consuming()
        except:
            self.channel.stop_consuming()

        self.connection.close()

if __name__ == '__main__':
    ProcessDispatcherAgent().start()
