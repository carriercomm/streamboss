#!/usr/bin/env python

import logging
import os
import sys
import uuid
import json
import time
import pika
import requests
import threading
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

S3_FILE_PREFIX = "___STREAMBOSS_S3_FILE___"
EXCHANGE_PREFIX = "streams"

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
                  RABBITMQ_EXCHANGE, ssl=False, sysname=None)
        self.pd_client = PDClient(self.dashi)
        self.pd_client.dashi_name = 'process_dispatcher'

        self.archived_streams = {}  # TODO: this sucks
        try:
            self.s3_connection = self._get_s3_connection()
        except Exception:
            print >> sys.stderr, "Couldn't get s3 connection, continuing without"
            self.s3_connection = None
        self.operations = {}

        self.dashi.handle(self.create_stream)
        self.dashi.handle(self.remove_stream)
        self.dashi.handle(self.create_operation)
        self.dashi.handle(self.remove_operation)
        self.dashi.handle(self.get_all_operations)
        self.dashi.handle(self.get_all_streams)

        self.dashi_thread = threading.Thread(target=self.dashi.consume)
        self.dashi_thread.start()

    def get_all_streams(self):
        streams = {}
        self.db_curs.execute("SELECT stream_name,archive FROM streams")
        for stream_name, archive in self.db_curs.fetchall():
            streams[stream_name] = {'archive': archive}
        return streams

    def get_all_operations(self):
        streams = {}
        self.db_curs.execute("SELECT routekey,created,process_definition_id,input_stream,output_stream,archive FROM operations")
        for name, created, process_definition_id, input_stream, output_stream, archive in self.db_curs.fetchall():
            streams[name] = {
                "created": created,
                "process_definition_id": process_definition_id,
                "input_stream": input_stream,
                "output_stream": output_stream,
                "archive": archive,
            }
        return streams

    def get_operation(self, operation=None):
        self.db_curs.execute(
            "SELECT process_definition_id,input_stream,output_stream,process, process_id, archive " +
            "FROM operations WHERE routekey='%s';" %
            operation)
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

    def get_stream(self, stream=None):
        self.db_curs.execute(
            "SELECT stream_name,archive " +
            "FROM streams WHERE stream_name='%s';" %
            stream)
        stream_row = self.db_curs.fetchone()
        if stream_row is None:
            return None
        stream_dict = {
            "stream_name": stream_row[0],
            "archive": stream_row[1],
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
        self.db_curs.execute("UPDATE operations SET created=1, process_id='%s' WHERE routekey='%s';" % (
            process_id, stream))

    def mark_as_not_created(self, stream):
        self.db_curs.execute("UPDATE operations SET created=0, process_id=NULL WHERE routekey='%s';" % stream)

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

    def create_operation(self, operation_name=None, process_definition_id=None, input_stream=None, output_stream=None):
        self.db_curs.execute("INSERT INTO operations (routekey, process_definition_id, input_stream, output_stream, created) VALUES ('%s', '%s', '%s', '%s', 0)" % (operation_name, process_definition_id, input_stream, output_stream))  # noqa

    def remove_operation(self, operation_name=None):
        self.db_curs.execute("DELETE FROM operations where operations.routekey='%s'" % operation_name)

    def create_stream(self, stream_name=None):
        s = self.get_stream(stream_name)
        if s is None:
            q = "INSERT INTO streams (stream_name) VALUES ('%s') " % stream_name
            self.db_curs.execute(q)
        s = self.get_stream(stream_name)
        return s

    def remove_stream(self, stream_name=None):
        self.db_curs.execute("DELETE FROM streams where streams.stream_name='%s'" % stream_name)

    def start_archiving_stream(self, stream_name, bucket_name=DEFAULT_ARCHIVE_BUCKET,
            leave_messages_on_queue=True):
        """
        Archives a stream to an S3 compatible service. Each message will be
        saved in a file named archive-STREAMNAME-TIMESTAMP. In the future, this could
        be chunked by second/minute/hour etc.
        """
        if self.s3_connection is None:
            return
        try:
            bucket = self.s3_connection.get_bucket(bucket_name)
        except boto.exception.S3ResponseError:
            self.s3_connection.create_bucket(bucket_name)
            bucket = self.s3_connection.get_bucket(bucket_name)

        try:
            exchange_name = "%s.%s" % (EXCHANGE_PREFIX, stream_name)
            self.channel.exchange_declare(exchange=exchange_name, type='fanout')
        except Exception as e:
            print e
            log.exception("Couldn't declare exchange")
            return
        print "START ARCHIVING %s" % exchange_name

        archive_queue_name = "%s.%s" % ("archive", stream_name)
        archive_queue = self.channel.queue_declare(queue=archive_queue_name)
        self.channel.queue_bind(exchange=exchange_name,
                queue=archive_queue.method.queue)

        def archive_message(ch, method, properties, body):
            k = Key(bucket)
            filename = "archive-%s-%s" % (stream_name, str(time.time()))
            k.key = filename
            k.set_contents_from_string(body)
            print "Sent '%s' to %s" % (body, filename)

        consumer_key = self.channel.basic_consume(archive_message, queue=archive_queue_name)
        self.archived_streams[stream_name] = consumer_key
        print "ARCHIVING %s" % exchange_name

    def stop_archiving_stream(self, stream_name):

        consumer_tag = self.archived_streams[stream_name]
        self.channel.basic_cancel(consumer_tag=consumer_tag, nowait=False)
        self.db_curs.execute("UPDATE streams SET archive=0 WHERE stream_name='%s';" % stream_name)
        self.channel.queue_unbind(queue="archive.%s" % stream_name, exchange="%s.%s" % (EXCHANGE_PREFIX, stream_name))
        self.channel.queue_delete(queue="archive.%s" % stream_name)
        del self.archived_streams[stream_name]

    def archive_stream(self, stream_name):
        self.db_curs.execute("UPDATE streams SET archive=1 WHERE stream_name='%s';" % stream_name)

    def disable_archive_stream(self, stream_name):
        self.db_curs.execute("UPDATE streams SET archive=0 WHERE stream_name='%s';" % stream_name)

    def stream_archive(self, archived_stream_name, bucket_name=DEFAULT_ARCHIVE_BUCKET,
            stream_onto=None, start_time=None, end_time=None):

        if self.s3_connection is None:
            return

        if stream_onto is None:
            stream_onto = archived_stream_name

        try:
            exchange_name = "%s.%s" % (EXCHANGE_PREFIX, stream_onto)
            self.channel.exchange_declare(exchange=exchange_name, type='fanout')
        except Exception as e:
            print e
            log.exception("Couldn't declare exchange")
            return

        try:
            bucket = self.s3_connection.get_bucket(bucket_name)
        except boto.exception.S3ResponseError:
            raise Exception("Bucket '%s' is not available. Can't stream." % bucket_name)

        keys = bucket.get_all_keys()
        messages = {}
        for key in keys:
            filename = key.key
            name_parts = filename.split('-')
            if len(name_parts) != 3 or not filename.startswith("archive"):
                #archive filename isn't in a recognizable format
                continue
            _, stream_name, timestamp = name_parts
            if stream_name == archived_stream_name:
                message = key.get_contents_as_string()
                messages[timestamp] = message

        timestamps = messages.keys()
        timestamps.sort()
        for ts in timestamps:
            self.channel.basic_publish(exchange=exchange_name, body=messages[ts])

    def stream_file(self, stream_name, bucket_name, filename):

        if self.s3_connection is None:
            return

        try:
            exchange_name = "%s.%s" % (EXCHANGE_PREFIX, stream_name)
            self.channel.exchange_declare(exchange=exchange_name, type='fanout')
        except Exception as e:
            print e
            log.exception("Couldn't declare exchange")
            return

        try:
            self.s3_connection.get_bucket(bucket_name)
        except boto.exception.S3ResponseError:
            raise Exception("Bucket '%s' is not available. Can't stream." % bucket_name)

        message = "%s:s3://%s/%s" % (S3_FILE_PREFIX, bucket_name, filename)
        self.channel.basic_publish(exchange=exchange_name, body=message)

    def _get_s3_connection(self):
        """
        TODO: Only supports hotel for now.
        """
        conn = boto.s3.connection.S3Connection(is_secure=False, port=8888,
             host='svc.uc.futuregrid.org', debug=0, https_connection_factory=None,
             calling_format=boto.s3.connection.OrdinaryCallingFormat())
        return conn

    def _get_bindings(self, exchange):
        try:
            r = requests.get('http://%s:%s/api/bindings' % (
                RMQHOST, 15672),
                auth=(RABBITMQ_USER, RABBITMQ_PASSWORD))
        except Exception:
            log.exception("Problem connecting to process registry")
            return None
        all_bindings = r.json()
        wanted_bindings = []
        exchange_name = "%s.%s" % (EXCHANGE_PREFIX, exchange)
        for binding in all_bindings:
            # TODO: if a stream is archived, should a process be started?
            if binding['source'] == exchange_name and not binding['destination'].startswith('archive'):
                wanted_bindings.append(binding)
        return wanted_bindings

    def start(self):

        while True:
            streams = self.get_all_streams()
            operations = self.get_all_operations()
            try:
                for stream in streams:
                    stream_description = self.get_stream(stream)
                    stream_name = stream_description['stream_name']

                    if (stream_description['archive'] == 1 and
                            stream_name not in self.archived_streams.keys()):
                        self.start_archiving_stream(stream_name)
                    elif (stream_description['archive'] == 0 and
                            stream_name in self.archived_streams.keys()):
                        self.stop_archiving_stream(stream_name)

                    try:
                        exchange_name = "%s.%s" % (EXCHANGE_PREFIX, stream_description['stream_name'])
                        self.channel.exchange_declare(exchange=exchange_name, type='fanout')
                    except Exception as e:
                        print e
                        log.exception("Couldn't declare exchange")
                        continue

                for operation in operations:
                    operation_description = self.get_operation(operation)

                    if operation_description.get('input_stream') is not None:
                        self.create_stream(operation_description['input_stream'])
                    if operation_description.get('output_stream') is not None:
                        self.create_stream(operation_description['output_stream'])

                    bindings = self._get_bindings(operation_description['output_stream'])

                    message_count = 0
                    consumer_count = 0
                    for binding in bindings:
                        queue = self.channel.queue_declare(queue=binding['destination'], auto_delete=True)
                        message_count += queue.method.message_count
                        consumer_count += queue.method.consumer_count

                    created = operations[operation]["created"]
                    log.debug("%s: message_count %d, consumer_count %d, created %d" % (
                        operation, message_count, consumer_count, created))

                    if consumer_count > 0 and not created:
                        log.info("%s has consumers, launching its process" % operation)
                        if operation_description is None:
                            print "NO PROC DEF ID"

                        process_id = self.launch_process(operation_description)
                        self.mark_as_created(operation, process_id)
                    elif consumer_count == 0 and created:
                        log.info("%s has 0 consumers, terminating its processes" % operation)
                        process_id = operation_description.get('process_id')
                        if process_id:
                            self.terminate_process(process_id)
                        self.mark_as_not_created(operation)
                    elif consumer_count == 0 and not created:
                        pass  # This is an idle state

            except pika.exceptions.ChannelClosed, e:
                print e
                pass
            time.sleep(1)

if __name__ == '__main__':
    sb = StreamBoss()
    try:
        sb.start()
    except KeyboardInterrupt:
        sb.dashi.cancel()
        sb.dashi.disconnect()
