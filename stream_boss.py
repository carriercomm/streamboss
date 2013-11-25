#!/usr/bin/env python

import logging
import os
import uuid
import json
import time
import pika
import requests
import boto.s3.connection
import MySQLdb as sql

from ceiclient.client import PDClient
from dashi import DashiConnection
from boto.s3.key import Key

logging.basicConfig(level=logging.DEBUG)
log = logging

# Quiet 3rd party logging

pika_logger = logging.getLogger('pika')
pika_logger.setLevel("CRITICAL")

boto_logger = logging.getLogger('boto')
boto_logger.setLevel("CRITICAL")

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
RABBITMQ_EXCHANGE = os.environ.get('STREAMBOSS_RABBITMQ_EXCHANGE', 'default_dashi_exchange')

PROCESS_REGISTRY_HOSTNAME = os.environ.get('PROCESS_REGISTRY_HOSTNAME', 'localhost')
PROCESS_REGISTRY_PORT = int(os.environ.get('PROCESS_REGISTRY_PORT', 8080))
PROCESS_REGISTRY_USERNAME = os.environ.get('PROCESS_REGISTRY_USERNAME', 'guest')
PROCESS_REGISTRY_PASSWORD = os.environ.get('PROCESS_REGISTRY_PASSWORD', 'guest')

DEFAULT_ARCHIVE_BUCKET = "stream_archive"

STREAM_AGENT_PATH = os.environ.get("STREAM_AGENT_PATH",
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "stream_agent.py"))


class StreamBoss(object):

    def __init__(self):
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        self.connection_parameters = pika.ConnectionParameters(RMQHOST, credentials=credentials)
        self.connection = pika.BlockingConnection(self.connection_parameters)
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
        self.pd_client.dashi_name = 'process_dispatcher'

        self.archived_streams = {}  # TODO: this sucks
        self.s3_connection = self._get_s3_connection()

    def get_all_streams(self):
        streams = {}
        self.db_curs.execute("SELECT routekey, created FROM streams")
        for name, created in self.db_curs.fetchall():
            streams[name] = {"created": created}
        return streams

    def get_stream(self, stream):
        self.db_curs.execute(
            "SELECT process_definition_id,input_stream,routekey,process, process_id, archive " +
            "FROM streams WHERE routekey='%s';" %
            stream)
        stream_row = self.db_curs.fetchone()
        stream_dict = {
            "process_definition_id": stream_row[0],
            "input_stream": stream_row[1],
            "output_stream": stream_row[2],
            "process": stream_row[3],
            "process_id": stream_row[4],
            "archive": stream_row[5]
        }
        return stream_dict

    def launch_process(self, stream_description):

        process_definition_id = stream_description["process_definition_id"]
        input_stream = stream_description["input_stream"]
        output_stream = stream_description["output_stream"]
        definition_id = output_stream

        SLOTS = 10

        process_definition = self.get_process_definition(process_definition_id)
        if process_definition is None:
            log.error("Couldn't get process definition %s?" % process_definition_id)
            return None

        deployable_type = process_definition.get('application', 'eeagent')
        process_exec = process_definition.get('exec', None)
        if process_exec is None:
            raise Exception("process definition has no exec")
        try:
            self.pd_client.add_engine(definition_id, SLOTS, **{'deployable_type': deployable_type})
        except Exception:
            log.exception("COULDN'T ADD ENGINE")

        stream_agent_path = STREAM_AGENT_PATH
        executable = {
            'exec': stream_agent_path,
            'argv': [
                "--rabbitmq-host %s" % RMQHOST,
                "--rabbitmq-user %s" % RABBITMQ_USER,
                "--rabbitmq-password %s" % RABBITMQ_PASSWORD,
                '\'%s\'' % json.dumps(process_definition),
                input_stream,
                output_stream
            ]
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

        process_id = "%s-%s" % (definition_id, uuid.uuid4().hex)
        self.pd_client.schedule_process(process_id, process_definition_id=definition_id,
                execution_engine_id=definition_id)

        return process_id

    def terminate_process(self, process_id):
        try:
            self.pd_client.terminate_process(process_id)
        except Exception:
            log.exception("Can't delete %s. Maybe already deleted?")

    def mark_as_created(self, stream, process_id):
        self.db_curs.execute("UPDATE streams SET created=1, process_id='%s' WHERE routekey='%s';" % (
            process_id, stream))

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

    def create_operation(self, operation_name, process_definition_id, input_stream, output_stream):
        self.db_curs.execute("INSERT INTO streams (routekey, process_definition_id, input_stream, output_stream, created) VALUES ('%s', '%s', '%s', '%s', 0)" % (operation_name, process_definition_id, input_stream, output_stream))  # noqa

    def remove_operation(self, operation_name):
        self.db_curs.execute("DELETE FROM streams where streams.routekey='%s'" % operation_name)

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

    def start_archiving_stream(self, stream_name, bucket_name=DEFAULT_ARCHIVE_BUCKET,
            leave_messages_on_queue=True):
        """
        Archives a stream to an S3 compatible service. Each message will be
        saved in a file named archive-STREAMNAME-TIMESTAMP. In the future, this could
        be chunked by second/minute/hour etc.
        """
        try:
            bucket = self.s3_connection.get_bucket(bucket_name)
        except boto.exception.S3ResponseError:
            self.s3_connection.create_bucket(bucket_name)
            bucket = self.s3_connection.get_bucket(bucket_name)

        def archive_message(ch, method, properties, body):
            k = Key(bucket)
            filename = "archive-%s-%s" % (stream_name, str(time.time()))
            k.key = filename
            k.set_contents_from_string(body)

        consumer_key = self.channel.basic_consume(archive_message, queue=stream_name)
        self.archived_streams[stream_name] = consumer_key

    def stop_archiving_stream(self, stream_name):

        consumer_tag = self.archived_streams[stream_name]
        self.channel.basic_cancel(consumer_tag=consumer_tag, nowait=False)
        self.db_curs.execute("UPDATE streams SET archive=0 WHERE routekey='%s';" % stream_name)
        del self.archived_streams[stream_name]

    def archive_stream(self, stream_name):
        self.db_curs.execute("UPDATE streams SET archive=1 WHERE routekey='%s';" % stream_name)

    def disable_archive_stream(self, stream_name):
        self.db_curs.execute("UPDATE streams SET archive=0 WHERE routekey='%s';" % stream_name)

    def _get_s3_connection(self):
        """
        TODO: Only supports hotel for now.
        """
        conn = boto.s3.connection.S3Connection(is_secure=False, port=8888,
             host='svc.uc.futuregrid.org', debug=0, https_connection_factory=None,
             calling_format=boto.s3.connection.OrdinaryCallingFormat())
        return conn

    def start(self):
        while True:
            streams = self.get_all_streams()
            try:
                for stream in streams:
                    stream_description = self.get_stream(stream)

                    if (stream_description['archive'] == 1 and
                            stream not in self.archived_streams.keys()):
                        self.start_archiving_stream(stream)
                    elif (stream_description['archive'] == 0 and
                            stream in self.archived_streams.keys()):
                        self.stop_archiving_stream(stream)

                    try:
                        status = self.channel.queue_declare(queue=stream)  # , passive=True)
                    except Exception as e:
                        print e
                        log.exception("Couldn't declare queue")
                        continue


                    message_count = status.method.message_count
                    consumer_count = status.method.consumer_count
                    if stream in self.archived_streams.keys():
                        consumer_count -= 1
                        consumer_count = max(0, consumer_count)
                    created = streams[stream]["created"]
                    log.debug("%s: message_count %d, consumer_count %d, created %d" % (
                        stream, message_count, consumer_count, created))

                    if consumer_count > 0 and not created:
                        log.info("%s has consumers, launching its process" % stream)
                        if stream_description is None:
                            print "NO PROC DEF ID"

                        process_id = self.launch_process(stream_description)
                        self.mark_as_created(stream, process_id)
                    elif consumer_count == 0 and created:
                        log.info("%s has 0 consumers, terminating its processes" % stream)
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
