#!/usr/bin/env python
import sys

from stream_boss import StreamBoss

USAGE = "USAGE: stream_archive.py streamname"

sb = StreamBoss()
try:
    sb.stream_archive(sys.argv[1])
except IndexError:
    sys.exit(USAGE)
