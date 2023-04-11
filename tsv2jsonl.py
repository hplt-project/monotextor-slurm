#!/usr/bin/env python3
from argparse import ArgumentParser
import json
import sys


parser = ArgumentParser("Convert tsv to jsonl")
parser.add_argument('-c', '--columns', required=True,
                    help='Column names separated by comma,'
                         ' odered as they appear in the file')
args = parser.parse_args()

fields = [n for n in args.columns.split(',')]

# Each line is readed into a dictionary, then parsed to json
for line in sys.stdin:
    parts = line.rstrip('\n').split('\t')

    jsonl = {}
    for i, field in enumerate(fields):
        jsonl[field] = parts[i]

    print(json.dumps(jsonl))
