#!/usr/bin/env python

import os
import sys
import json
import select
import argparse
import threading

from subprocess import PIPE, Popen, STDOUT

import pika

BUFSIZE = 4096
RMQHOST = os.environ.get('STREAMBOSS_RABBITMQ_HOST', 'localhost')
RABBITMQ_USER = os.environ.get('STREAMBOSS_RABBITMQ_USER', 'guest')
RABBITMQ_PASSWORD = os.environ.get('STREAMBOSS_RABBITMQ_PASSWORD', 'guest')

SINGLE_PROCESS_TYPE = 'single'
SERVICE_PROCESS_TYPE = 'service'
DEFAULT_PROCESS_TYPE = SINGLE_PROCESS_TYPE


class ProcessDispatcherAgent(object):

    def __init__(self):
        global RMQHOST
        global RABBITMQ_USER
        global RABBITMQ_PASSWORD

        parser = argparse.ArgumentParser()
        parser.add_argument("process_description")
        parser.add_argument("input_stream")
        parser.add_argument("output_stream")
        parser.add_argument("--rabbitmq-host")
        parser.add_argument("--rabbitmq-user")
        parser.add_argument("--rabbitmq-password")
        args = parser.parse_args()

        if args.rabbitmq_host:
            RMQHOST = args.rabbitmq_host
        if args.rabbitmq_user:
            RABBITMQ_USER = args.rabbitmq_user
        if args.rabbitmq_host:
            RABBITMQ_PASSWORD = args.rabbitmq_password

        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(RMQHOST, credentials=credentials))
        self.channel = self.connection.channel()

        self.process_definition = None

        # JSON used to be loaded from a file. Instead we get it directly as a string from argv[1]
        #with open(sys.argv[1]) as f:
            #json_data = f.read()
            #self.process_definition = json.loads(json_data)

        json_data = args.process_description
        self.process_definition = json.loads(json_data)

        if self.process_definition is None:
            sys.exit("No process definition provided")

        self.process_path = self.process_definition.get("exec")
        self.process_environment = dict(os.environ.copy().items() +
                self.process_definition.get("environment", {}).items())
        self.process_type = self.process_definition.get("process_type", DEFAULT_PROCESS_TYPE)

        self.input_stream = args.input_stream
        self.output_stream = args.output_stream

        self.exchange_name = 'streams'

        self.exchange = self.channel.exchange_declare(exchange=self.exchange_name, auto_delete=True)

        self.input_queue = self.channel.queue_declare(queue=self.input_stream)
        self.output_queue = self.channel.queue_declare(queue=self.output_stream)
        self.channel.queue_bind(exchange=self.exchange_name,
                queue=self.input_queue.method.queue, routing_key=self.input_stream)
        self.channel.queue_bind(exchange=self.exchange_name,
                queue=self.output_queue.method.queue, routing_key=self.output_stream)

    def single_consume_func(self, ch, method, properties, body):
        if method.exchange == "streams":
            message = body
            print "< got: %s" % message
        else:
            print "< got unrecognized message"
            return

        self.p = Popen(self.process_path, shell=True, bufsize=BUFSIZE, env=self.process_environment,
                stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True, cwd="/tmp")
        (child_stdin, child_stdout, child_stderr) = (self.p.stdin, self.p.stdout, self.p.stderr)
        child_stdin.write(message + "\n")
        child_stdin.flush()
        # TODO: differentiate between ongoing and one-shot
        child_stdin.close()
        returncode = self.p.wait()
        output = child_stdout.read()
        if output[-1] == '\n':
            output = output[:-1]
        print "> sending: %s" % output
        if returncode == 0:
            self.channel.basic_publish(exchange='streams', routing_key=self.output_stream, body=output)
        else:
            print "Error running %s: returned %d" % (self.process_path, returncode)

    def service_consume_func(self, ch, method, properties, body):
        if method.exchange == "streams":
            message = body
            print "< got: %s" % message
        else:
            print "< got unrecognized message"
            return

        (child_stdin, child_stdout, child_stderr) = (self.p.stdin, self.p.stdout, self.p.stderr)
        child_stdin.write(message + "\n")
        child_stdin.flush()

    def _read_from_process(self):

        while self.p.poll() is None:
            ready, _, _ = select.select([self.p.stdout], [], [], 1)
            if ready:
                line = self.p.stdout.readline()
                print "> sending: %s" % line
                self.channel.basic_publish(exchange='streams', routing_key=self.output_stream, body=line)
        else:
            print "process ended with %s" % self.p.poll()

    def start(self):

        if self.process_type == SERVICE_PROCESS_TYPE:
            self.p = Popen(self.process_path, shell=True, bufsize=BUFSIZE,
                    env=self.process_environment,
                    stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True, cwd="/tmp")
            self.channel.basic_consume(self.service_consume_func, queue=self.input_queue.method.queue, no_ack=True)
            self.read_thread = threading.Thread(target=self._read_from_process)
            self.read_thread.run()

        elif self.process_type == SINGLE_PROCESS_TYPE:
            self.channel.basic_consume(self.single_consume_func, queue=self.input_queue.method.queue, no_ack=True)

        try:
            self.channel.start_consuming()
        except:
            self.channel.stop_consuming()

        self.p.kill()
        self.p.wait()
        self.connection.close()

if __name__ == '__main__':
    ProcessDispatcherAgent().start()
