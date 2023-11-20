#!/usr/bin/env python
import sys

ln = set(int(i) for i in sys.argv[1:])

for i, l in enumerate(sys.stdin):
    if i in ln:
        print(l)
