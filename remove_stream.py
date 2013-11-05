#!/usr/bin/env python
import sys

from stream_boss import StreamBoss

USAGE = "USAGE: remove_stream.py streamname"

sb = StreamBoss()
try:
    sb.remove_stream(sys.argv[1])
except IndexError:
    sys.exit(USAGE)
