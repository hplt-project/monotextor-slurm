#!/usr/bin/env python3
from subprocess import Popen, PIPE, TimeoutExpired
from argparse import ArgumentParser
from os.path import join as pjoin
import binascii
import gzip
import base64
import sys
import os


parser = ArgumentParser(description="Gather collection name, URL and text "
                        " from warc2text batches."
                        " Output a tab-separated file to stderr."
                        "\nFormat: url,paragraph,<paragraph_id>,collection")
parser.add_argument('input_dir', type=str,
                    help='warc2text subdirectory where text.gz and url.gz re stored')
parser.add_argument('-c', '--collection', required=True, type=str,
                    help='Collection name where text comes from')
args = parser.parse_args()

print(f"Reading {args.input_dir}", file=sys.stderr)


with gzip.open(pjoin(args.input_dir, 'plain_text.gz'), 'rb') as p, \
        gzip.open(pjoin(args.input_dir, 'url.gz'), 'rb') as u:

    # Each document in a line base64 encoded
    # propagate url and collection for each document line
    # tab-separated text compressed, splitted in batchs of SIZE
    for i, doc in enumerate(p):
        # read b64 encoded documents and their urls
        try:
            docurl = u.readline().strip() \
                        .decode('utf-8', errors='strict')
            lines = base64.b64decode(doc.strip()) \
                        .decode('utf-8', errors='strict').split('\n')
        except (UnicodeDecodeError, binascii.Error) as e:
            print(f"WARNING: unicode error in doc {i} of {args.input_dir}",
                    file=sys.stderr)

        # Print each document with its url and collection
        # use \b to keep document in one line (will be repalced by \n afterwards)
        printed = False
        for line in lines:
            if line:
                printed = True
                line = line.replace('\b', '')
                print(f'{docurl}\t{line}\t{args.collection}', end='\b')
        if printed:
            print("")
