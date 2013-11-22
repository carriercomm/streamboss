#!/usr/bin/env python
import sys

from stream_boss import StreamBoss

USAGE = "USAGE: create_operation.py operationname process_definition_id inputstream outputstream"

sb = StreamBoss()
try:
    sb.create_operation(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
except IndexError:
    sys.exit(USAGE)
