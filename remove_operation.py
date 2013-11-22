#!/usr/bin/env python
import sys

from stream_boss import StreamBoss

USAGE = "USAGE: remove_operation.py operationname"

sb = StreamBoss()
try:
    sb.remove_operation(sys.argv[1])
except IndexError:
    sys.exit(USAGE)
