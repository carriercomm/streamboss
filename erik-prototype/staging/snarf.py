snarf.py:

call initialize

bind to announce queue with callback handle_announce()
bind to snarf queue with callback handle_snarf()

call begin_consuming()

----------
handle_announce():

create DB table for new stream
confirm table ready?
bind to $queue_name with callback handle_instream()

----------
handle_snarf():

... I'll remember why this is here eventually

----------
handle_instream():

find table name matching stream ID
db.insert into table values(...)

----------
initialize():

connect to $rmq_server
declare 'snarf' queue
declare 'ann_stream' queue
connect to $sql_server

