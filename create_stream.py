#!/usr/bin/env python
import sys

from stream_boss import StreamBoss

USAGE = "USAGE: create_stream.py stream_name"

sb = StreamBoss()
try:
    sb.create_stream(sys.argv[1])
except IndexError:
    sys.exit(USAGE)
