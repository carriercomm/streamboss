#!/usr/bin/env python

import logging
import os
import time

import MySQLdb as sql
import pika
import requests

from ceiclient.client import PDClient
from dashi import DashiConnection
#from tasks import run

logging.basicConfig(level=logging.DEBUG)
log = logging

# Quiet 3rd party logging

pika_logger = logging.getLogger('pika')
pika_logger.setLevel("CRITICAL")

dashi_logger = logging.getLogger('dashi')
dashi_logger.setLevel("INFO")

amqp_logger = logging.getLogger('amqp')
amqp_logger.setLevel("INFO")

requests_logger = logging.getLogger('requests')
requests_logger.setLevel("CRITICAL")

DBHOST = os.environ.get('STREAMBOSS_DBHOST', 'localhost')
DBUSER = os.environ.get('STREAMBOSS_DBUSER', 'guest')
DBPASS = os.environ.get('STREAMBOSS_DBPASS', 'guest')
DBDB = os.environ.get('STREAMBOSS_DBNAME', 'streamboss')

RMQHOST = os.environ.get('STREAMBOSS_RABBITMQ_HOST', 'localhost')
RMQPORT = os.environ.get('STREAMBOSS_RABBITMQ_PORT', 5672)
RABBITMQ_USER = os.environ.get('STREAMBOSS_RABBITMQ_USER', 'guest')
RABBITMQ_PASSWORD = os.environ.get('STREAMBOSS_RABBITMQ_PASSWORD', 'guest')
RABBITMQ_VHOST = os.environ.get('STREAMBOSS_RABBITMQ_VHOST', '/')

PROCESS_REGISTRY_HOSTNAME = os.environ.get('PROCESS_REGISTRY_HOSTNAME', 'localhost')
PROCESS_REGISTRY_PORT = int(os.environ.get('PROCESS_REGISTRY_PORT', 8080))
PROCESS_REGISTRY_USERNAME = os.environ.get('PROCESS_REGISTRY_USERNAME', 'guest')
PROCESS_REGISTRY_PASSWORD = os.environ.get('PROCESS_REGISTRY_PASSWORD', 'guest')

AGENT_PATH = '/Users/priteau/Work/process-dispatcher/agent.py'
STREAM_AGENT_PATH = os.environ.get("STREAM_AGENT_PATH",
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "stream_agent.py"))


class StreamBoss(object):

    def __init__(self):
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(RMQHOST, credentials=credentials))
        self.channel = self.connection.channel()

        self.db_connection = sql.connect(DBHOST, DBUSER, DBPASS, DBDB)
        self.db_connection.autocommit(True)
        self.db_curs = self.db_connection.cursor()

        self.dashi = DashiConnection("streamboss",
                  'amqp://%s:%s@%s:%s//' % (
                      RABBITMQ_USER,
                      RABBITMQ_PASSWORD, RMQHOST,
                      RMQPORT),
                  "default_dashi_exchange", ssl=False, sysname=None)
        self.pd_client = PDClient(self.dashi)
        self.pd_client.dashi_name = 'pd_0'

    def get_all_streams(self):
        streams = {}
        self.db_curs.execute("SELECT routekey, created FROM streams")
        for name, created in self.db_curs.fetchall():
            streams[name] = {"created": created}
        return streams

    def get_stream(self, stream):
        self.db_curs.execute(
            "SELECT process_definition_id,input_stream,routekey,process,process_id FROM streams WHERE routekey='%s';" %
            stream)
        stream_row = self.db_curs.fetchone()
        stream_dict = {
            "process_definition_id": stream_row[0],
            "input_stream": stream_row[1],
            "output_stream": stream_row[2],
            "process": stream_row[3],
            "process_id": stream_row[4]
        }
        return stream_dict

    def launch_process(self, stream_description):
        process_definition_id = stream_description["process_definition_id"]
        input_stream = stream_description["input_stream"]
        output_stream = stream_description["output_stream"]

        process_definition = self.get_process_definition(process_definition_id)
        if process_definition is None:
            log.error("Couldn't get process definition %s?" % process_definition_id)
            return None

        definition_id = output_stream
        stream_agent_path = STREAM_AGENT_PATH
        executable = {
            'exec': stream_agent_path,
            'argv': ['\'{"exec": "tr \'[a-z]\' \'[A-Z]\'"}\'', input_stream, output_stream]
        }
        definition = {
            'definition_type': 'supd',
            'name': definition_id,
            'description': "some process",
            'executable': executable,
        }

        try:
            self.pd_client.create_process_definition(process_definition=definition,
                process_definition_id=definition_id)
        except Exception:
            self.pd_client.update_process_definition(process_definition=definition,
                process_definition_id=definition_id)

        process_id = definition_id
        self.pd_client.schedule_process(process_id, process_definition_id=definition_id)

        return process_id

    def terminate_process(self, process_id):
        try:
            self.pd_client.terminate_process(process_id)
        except Exception:
            log.exception("Can't delete %s. Maybe already deleted?")

    def mark_as_created(self, stream, process_id):
        self.db_curs.execute("UPDATE streams SET created=1, process_id='%s' WHERE routekey='%s';" % (
            stream, process_id))

    def mark_as_not_created(self, stream):
        self.db_curs.execute("UPDATE streams SET created=0, process_id=NULL WHERE routekey='%s';" % stream)

    def get_process_definition(self, process_definition_id):
        try:
            r = requests.get('http://%s:%s/api/process_definition/%d' % (
                PROCESS_REGISTRY_HOSTNAME, PROCESS_REGISTRY_PORT, process_definition_id),
                auth=(PROCESS_REGISTRY_USERNAME, PROCESS_REGISTRY_PASSWORD))
        except Exception:
            log.exception("Problem connecting to process registry")
            return None

        if r.status_code == 200:
            if r.headers['content-type'] == "application/json":
                process_definition = r.json().get("definition")
            else:
                log.error("Definition %d not json: %s" % (process_definition_id, r.text))
                return None
        else:
            log.error("Could not find definition %d" % process_definition_id)
            return None

        return process_definition

    def create_stream(self, stream_name, process_definition_id, input_stream, output_stream):
        self.db_curs.execute("INSERT INTO streams (routekey, process_definition_id, input_stream, output_stream, created) VALUES ('%s', '%s', '%s', '%s', 0)" % (stream_name, process_definition_id, input_stream, output_stream))  # noqa

    def remove_stream(self, stream_name):
        self.db_curs.execute("DELETE FROM streams where streams.routekey='%'" % stream_name)

    def activate_stream(self):
        """
        When there are subscribers to a stream, we should start a stream process
        via the PD to start processing it
        """
        pass

    def deactivate_stream(self):
        """
        When there no subscribers to a stream, we should terminate any stream processes
        via the PD
        """
        pass

    def start(self):
        while True:
            streams = self.get_all_streams()
            try:
                for stream in streams:
                    try:
                        status = self.channel.queue_declare(queue=stream)  # , passive=True)
                    except Exception as e:
                        print e
                        log.exception("Couldn't declare queue")
                        continue
                    message_count = status.method.message_count
                    consumer_count = status.method.consumer_count
                    created = streams[stream]["created"]
                    log.debug("%s: message_count %d, consumer_count %d, created %d" % (
                        stream, message_count, consumer_count, created))

                    if consumer_count > 0 and not created:
                        log.info("%s has consumers, launching its process" % stream)
                        stream_description = self.get_stream(stream)
                        if stream_description is None:
                            print "NO PROC DEF ID"

                        process_id = self.launch_process(stream_description)
                        self.mark_as_created(stream, process_id)
                    elif consumer_count == 0 and created:
                        log.info("%s has 0 consumers, terminating its processes" % stream)
                        stream_description = self.get_stream(stream)
                        process_id = stream_description.get('process_id')
                        if process_id:
                            self.terminate_process(process_id)
                        self.mark_as_not_created(stream)
                    elif consumer_count == 0 and not created:
                        pass  # This is an idle state

            except pika.exceptions.ChannelClosed, e:
                print e
                pass
            time.sleep(1)

if __name__ == '__main__':
    StreamBoss().start()
