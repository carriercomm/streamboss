#!/usr/bin/env python
#Author: Nick Bond
#Purpose: This script allows the user to connect to an S3 cloud storage source
#	  and then create a new bucket and key. After that the user is able to
#	  upload a file and append the bucket with the new key that houses their
#	  file. A MySQL dump is used for this example

import os
import uuid

import boto
import boto.s3.connection

def object_creator(): 
	
	generator = uuid.uuid4()
	return generator	


filename = 'mysqlbackup.sql'

access_key_id = os.environ['AWS_ACCESS_KEY_ID']
secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']

##Connecting to cloud storage##

conn=boto.s3.connection.S3Connection(aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key, #Connecting to Cumulus
is_secure=False, port=8888, host='hostname',
debug=0, https_connection_factory=None, calling_format = boto.s3.connection.OrdinaryCallingFormat())

##Connecting to an existing bucket and creating a new key##
bucket = conn.get_bucket('anltowerdata') 
key = bucket.new_key('mysqlbackup.sql')
key.set_contents_from_filename(filename)
key = bucket.get_key('mysqlbackup.sql')
key.set_canned_acl('public-read')
key = bucket.get_key('mysqlbackup.sql')
key_url = key.generate_url(0, query_auth=False, force_http=True)
print key_url
for key in bucket.list():
        print "{name}\t{size}\t{modified}".format(
                name = key.name,
                size = key.size,
                modified = key.last_modified,
                )
