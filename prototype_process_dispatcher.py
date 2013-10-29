#!/usr/bin/env python

import json
#import logging
import os
import shlex
import subprocess
import tempfile
import time
import uuid

import MySQLdb as sql
import pika
import requests

from tasks import run

#logging.basicConfig(level=logging.DEBUG)

DBHOST = os.environ['STREAMBOSS_DBHOST']
DBUSER = os.environ['STREAMBOSS_DBUSER']
DBPASS = os.environ['STREAMBOSS_DBPASS']
DBDB = os.environ['STREAMBOSS_DBNAME']

PHANTOM_USER_ID = os.environ['USER_ID']
PHANTOM_TOKEN = os.environ['TOKEN']
PHANTOM_API_URL = 'https://phantom.nimbusproject.org/api/dev'
PHANTOM_CLOUD_NAME = 'hotel'

RMQHOST = os.environ['STREAMBOSS_RABBITMQ_HOST']
RABBITMQ_USER = os.environ['STREAMBOSS_RABBITMQ_USER']
RABBITMQ_PASSWORD = os.environ['STREAMBOSS_RABBITMQ_PASSWORD']

PROCESS_REGISTRY_HOSTNAME = os.environ['PROCESS_REGISTRY_HOSTNAME']
PROCESS_REGISTRY_PORT = os.environ['PROCESS_REGISTRY_PORT']
PROCESS_REGISTRY_USERNAME = os.environ['PROCESS_REGISTRY_USERNAME']
PROCESS_REGISTRY_PASSWORD = os.environ['PROCESS_REGISTRY_PASSWORD']

AGENT_PATH = '/Users/priteau/Work/process-dispatcher/agent.py'

stream_processes = {}
stream_agents = {}


class ProcessState(object):
    """Valid states for processes in the system

    In addition to this state value, each process also has a "round" number.
    This is the number of times the process has been assigned a slot and later
    been ejected (due to failure perhaps).

    These two values together move only in a single direction, allowing
    the system to detect and handle out-of-order messages. The state values are
    ordered and any backwards movement will be accompanied by an increment of
    the round.

    So for example a new process starts in Round 0 and state UNSCHEDULED and
    proceeds through states as it launches:

    Round   State

    0       100-UNSCHEDULED
    0       200-REQUESTED
    0       300-WAITING             process is waiting in a queue
    0       400-PENDING             process is assigned a slot and deploying

    Unfortunately the assigned resource spontaneously catches on fire. When
    this is detected, the process round is incremented and state rolled back
    until a new slot can be assigned. Perhaps it is at least given a higher
    priority.

    1       250-DIED_REQUESTED      process is waiting in the queue
    1       400-PENDING             process is assigned a new slot
    1       500-RUNNING             at long last

    The fire spreads to a neighboring node which happens to be running the
    process. Again the process is killed and put back in the queue.

    2       250-DIED_REQUESTED
    2       300-WAITING             this time there are no more slots


    At this point the client gets frustrated and terminates the process to
    move to another datacenter.

    2       600-TERMINATING
    2       700-TERMINATED

    """

    UNSCHEDULED = "100-UNSCHEDULED"
    """Process has been created but not scheduled to run. It will not be
    scheduled until requested by the user
    """

    UNSCHEDULED_PENDING = "150-UNSCHEDULED_PENDING"
    """Process is unscheduled but will be automatically scheduled in the
    future. This is used by the Doctor role to hold back some processes
    during system bootstrap.
    """

    REQUESTED = "200-REQUESTED"
    """Process request has been acknowledged by Process Dispatcher

    The process is pending a decision about whether it can be immediately
    assigned a slot or if it must wait for one to become available.
    """

    DIED_REQUESTED = "250-DIED_REQUESTED"
    """Process was >= PENDING but died, waiting for a new slot

    The process is pending a decision about whether it can be immediately
    assigned a slot or if it must wait for one to become available (or
    be rejected).
    """

    WAITING = "300-WAITING"
    """Process is waiting for a slot to become available

    There were no available slots when this process was reviewed by the
    matchmaker. Processes which request not to be queued, by their
    queueing mode flag will never reach this state and will go directly to
    REJECTED.
    """

    ASSIGNED = "350-ASSIGNED"
    """Process is deploying to a slot

    Process is assigned to a slot and deployment is underway. Once a
    process reaches this state, moving back to an earlier state requires an
    increment of the process' round.

    """

    PENDING = "400-PENDING"
    """Process is starting on a resource

    A process has been deployed to a slot and is starting.
    """

    RUNNING = "500-RUNNING"
    """Process is running
    """

    TERMINATING = "600-TERMINATING"
    """Process termination has been requested
    """

    TERMINATED = "700-TERMINATED"
    """Process is terminated
    """

    EXITED = "800-EXITED"
    """Process has finished execution successfully
    """

    FAILED = "850-FAILED"
    """Process request failed
    """

    REJECTED = "900-REJECTED"
    """Process could not be scheduled and it was rejected

    This is the terminal state of processes with queueing mode NEVER and
    RESTART_ONLY when no resources are immediately available, or START_ONLY
    when there are no resources immediately available on restart
    """

    TERMINAL_STATES = (UNSCHEDULED, UNSCHEDULED_PENDING, TERMINATED, EXITED,
                       FAILED, REJECTED)
    """Process states which will not change without a request from outside.
    """


class Process(object):
    def __init__(self, stream_description):
        self.process_definition_id = stream_description["process_definition_id"]
        self.input_stream = stream_description["input_stream"]
        self.output_stream = stream_description["output_stream"]

        self.state = ProcessState.UNSCHEDULED
        self.round = 0
        self.uuid = str(uuid.uuid4())

    def change_state(self, new_state):
        self.state = new_state

    def increase_round(self):
        self.round += 1


class ProcessDispatcher(object):

    def __init__(self):
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(RMQHOST, credentials=credentials))
        self.channel = self.connection.channel()

        self.db_connection = sql.connect(DBHOST, DBUSER, DBPASS, DBDB)
        self.db_connection.autocommit(True)
        self.db_curs = self.db_connection.cursor()

    def get_all_streams(self):
        streams = {}
        self.db_curs.execute("SELECT * FROM streams")
        self.db_curs.execute("SELECT routekey, created FROM streams")
        for name, created in self.db_curs.fetchall():
            streams[name] = {"created": created}
        return streams

    def get_stream(self, stream):
        self.db_curs.execute("SELECT * FROM streams WHERE routekey='%s';" % stream)
        stream_row = self.db_curs.fetchone()
        return {"output_stream": stream_row[0], "process_definition_id": stream_row[1], "input_stream": stream_row[2]}

    def launch_process(self, stream_description):
        process_definition_id = stream_description["process_definition_id"]
        input_stream = stream_description["input_stream"]
        output_stream = stream_description["output_stream"]

        process_definition = self.get_process_definition(process_definition_id)
        if process_definition is not None:
            run.delay(json.dumps(process_definition), input_stream, output_stream)
            #fh, definition_file_path = tempfile.mkstemp()
            #with os.fdopen(fh, 'w') as d:
                #d.write(json.dumps(process_definition))

            #cmd = "%s %s %s %s" % (AGENT_PATH, definition_file_path, input_stream, output_stream)
            #args = shlex.split(str(cmd))
            #p = subprocess.Popen(args, close_fds=True)
            #print p
            #stream_agents[output_stream] = p

    def terminate_process(self, stream):
        p = stream_agents.get(stream)
        if p is not None:
            p.terminate()
        else:
            print "Warning: could not get execution process for stream %s" % stream

    def mark_as_created(self, stream):
        self.db_curs.execute("UPDATE streams SET created=1 WHERE routekey='%s';" % stream)

    def mark_as_not_created(self, stream):
        self.db_curs.execute("UPDATE streams SET created=0 WHERE routekey='%s';" % stream)

    def get_process_definition(self, process_definition_id):
        r = requests.get('http://%s:%s/api/process_definition/%d' % (PROCESS_REGISTRY_HOSTNAME, PROCESS_REGISTRY_PORT, process_definition_id),
                         auth=(PROCESS_REGISTRY_USERNAME, PROCESS_REGISTRY_PASSWORD))
        if r.status_code == 200:
            if r.headers['content-type'] == "application/json":
                process_definition = r.json().get("definition")
            else:
                print "Definition %d not json: %s" % (process_definition_id, r.text)
                return None
        else:
            print "Could not find definition %d" % process_definition_id
            return None

        return process_definition

    def create_lc(self, lc_name, cloud_name, stream_description):
        process_definition_id = stream_description["process_definition_id"]
        input_stream = stream_description["input_stream"]
        output_stream = stream_description["output_stream"]

        new_lc = {
            "name": lc_name,
            "cloud_params": {
                cloud_name: {
                    "image_id": "Debian-6.0.7-amd64-userdata.gz",
                    "instance_type": "m1.small",
                    "max_vms": -1,
                    "common": True,
                    "rank": 1
                }
            },
            "contextualization_method": "chef",
            "chef_attributes": {
                "observatories": {
                    "agent": {
                        "process_definition": "{\"exec\": \"/root/generate_random.py\"}",
                        "output_stream": "test_double",
                        "input_stream": "test"
                    },
                    "rabbitmq": {
                        "username": RABBITMQ_USER,
                        "password": RABBITMQ_PASSWORD,
                        "hostname": RMQHOST
                    }
                }
            },
            "chef_runlist": [
                "recipe[apt]",
                "recipe[observatories::agent]"
            ]
        }

        r = requests.post("%s/launchconfigurations" % PHANTOM_API_URL, data=json.dumps(new_lc), auth=(PHANTOM_USER_ID, PHANTOM_TOKEN))
        if r.status_code != 201:
            print "Failed to create LC: %s" % r.text
            return None

        return r.json().get('id')

    def create_domain(self, lc_name, domain_name):
        lc_name = lc_name
        vm_count = 0
        de_name = "multicloud"

        new_domain = {
            'name': domain_name,
            'de_name': de_name,
            'lc_name': lc_name,
            'vm_count': vm_count
        }

        r = requests.post("%s/domains" % PHANTOM_API_URL, data=json.dumps(new_domain), auth=(PHANTOM_USER_ID, PHANTOM_TOKEN))
        if r.status_code != 201:
            print "Error: %s" % r.text
            return None
        else:
            return r.json()["id"]

    def scale_domain(self, domain_id, n):
        r = requests.get("%s/domains/%s" % (PHANTOM_API_URL, domain_id), auth=(PHANTOM_USER_ID, PHANTOM_TOKEN))
        if r.status_code != 200:
            print "Error: %s" % r.text

        domain_to_change = r.json()
        if domain_to_change['vm_count'] != n:
            domain_to_change['vm_count'] = n
        else:
            return domain_to_change

        print "Scaling domain to %d VMs" % n
        r = requests.put("%s/domains/%s" % (PHANTOM_API_URL, domain_id), data=json.dumps(domain_to_change), auth=(PHANTOM_USER_ID, PHANTOM_TOKEN))
        if r.status_code != 200:
            print "Error: %s" % r.text
            return None
        else:
            return r.json()

    def terminate_domain(self, domain_name):
        domain_id = self.get_domain_by_name(domain_name)
        r = requests.delete("%s/domains/%s" % (PHANTOM_API_URL, domain_id), auth=(PHANTOM_USER_ID, PHANTOM_TOKEN))

        if r.status_code != 204:
            print "Problem deleting domain %s" % r.text

    def get_domain_status(self, domain_id):
        r = requests.get("%s/domains/%s" % (PHANTOM_API_URL, domain_id), auth=(PHANTOM_USER_ID, PHANTOM_TOKEN))
        if r.status_code != 200:
            print "Error: %s" % r.text
            return None
        else:
            print r.json()

    def get_running_instances(self, domain_id):
        r = requests.get("%s/domains/%s/instances" % (PHANTOM_API_URL, domain_id), auth=(PHANTOM_USER_ID, PHANTOM_TOKEN))
        if r.status_code != 200:
            print "Error: %s" % r.text
            return None
        else:
            all_instances = r.json()
            return filter(lambda x: x["lifecycle_state"] == "600-RUNNING", all_instances)

    def get_domain_by_name(domain_name):
        r = requests.get("%s/domains" % PHANTOM_API_URL, auth=(PHANTOM_USER_ID, PHANTOM_TOKEN))
        all_domains = r.json()

        domain_id = None
        for domain in all_domains:
            if domain.get('name') == domain_name:
                domain_id = domain.get('id')
                break

        return domain_id

    def start(self):
        while True:
            streams = self.get_all_streams()
            try:
                for stream in streams:
                    status = self.channel.queue_declare(queue=stream, passive=True)
                    message_count = status.method.message_count
                    consumer_count = status.method.consumer_count
                    created = streams[stream]["created"]
                    print "%s: message_count %d, consumer_count %d, created %d" % (stream, message_count, consumer_count, created)

                    # Check for new streams or closed streams
                    #if consumer_count > 0 and not created:
                        #stream_description = self.get_stream(stream)
                        #process = Process(stream_description)
                        #stream_processes[stream] = process
                        #self.create_lc(stream, PHANTOM_CLOUD_NAME, stream_description)
                        #self.create_domain(stream, stream)
                        #self.mark_as_created(stream)
                    #elif consumer_count == 0 and created:
                        #self.terminate_domain(stream)
                        #self.mark_as_not_created(stream)
                    #elif consumer_count > 0 and created:
                        #domain_id = self.get_domain_by_name(stream)
                        #if domain_id is not None:
                            #running_instances = self.get_running_instances(domain_id)
                            #if running_instances is not None and len(running_instances) != 0:
                                #process = stream_processes[stream]
                                #state = process.state
                                #if state == ProcessState.UNSCHEDULED:
                                    #if len(running_instances) > 0:
                                        #self.launch_process(stream_description, running_instances.pop)
                                        #process.change_state(ProcessState.PENDING)
                                    #else:
                                        #print "No running instance for executing process %s" % process.uuid
                            #else:
                                #self.scale_domain(domain_id, 1)

                    if consumer_count > 0 and not created:
                        stream_description = self.get_stream(stream)
                        process = Process(stream_description)
                        stream_processes[stream] = process
                        #self.create_lc(stream, PHANTOM_CLOUD_NAME, stream_description)
                        #self.create_domain(stream, stream)
                        self.launch_process(stream_description)
                        self.mark_as_created(stream)
                    elif consumer_count == 0 and created:
                        #self.terminate_domain(stream)
                        self.mark_as_not_created(stream)

            except pika.exceptions.ChannelClosed, e:
                print e
                pass
            time.sleep(1)

if __name__ == '__main__':
    ProcessDispatcher().start()
