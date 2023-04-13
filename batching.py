#!/usr/bin/env python3
from argparse import ArgumentParser
from os.path import join as pjoin
import gzip
import base64
import sys
import os

import zstandard


SIZE = 512 * 1024 * 1024

parser = ArgumentParser(description="Gather collection name, URL and text "
                        " from warc2text batches."
                        " Split lines into batches."
                        " Output a tab-separated file."
                        "\nFormat: url,paragraph,<paragraph_id>,collection")
parser.add_argument('-l', '--lang', type=str,
                    required=True, help='Language to process')
parser.add_argument('-s', '--size', type=int, default=SIZE,
                    help='Size of batches in number of characters (approximated)')
parser.add_argument('directory', type=str,
                    help='warc2text directory where collections are stored')
parser.add_argument('output_dir', type=str,
                    help='Output directory to store pieces.'
                         ' Will follow the structure {output}/{lang}/batch.{num}.zst')
args = parser.parse_args()

# Create directory if it does not exist
try:
    os.mkdir(pjoin(args.output_dir, args.lang))
except FileExistsError:
    pass


ofp = None
n_chars = args.size # set to size, trigger file creation in the first step
batch = 0 # batch 0 will never be created
cctx = zstandard.ZstdCompressor(threads=2)

# Iterate over collections
for coll in sorted(os.listdir(args.directory)):
    # Check it is a collection dir
    if not os.path.isdir(pjoin(args.directory, coll)):
        continue

    # Convert directory numbers to int so they can be sorted numerically
    for dirnum in sorted(map(int, os.listdir(pjoin(args.directory, coll)))):
        dirnum = str(dirnum) # to string again

        # Check directory exist for that language
        curpath = pjoin(args.directory, coll, dirnum, args.lang)
        if not os.path.isdir(curpath):
            #print(f"WARNING: {coll}/{dirnum} does not exist", file=sys.stderr)
            continue
        print(f"Reading {coll}/{dirnum}", file=sys.stderr)

        # Read text and url files
        with gzip.open(pjoin(curpath, 'text.gz'), 'r') as p, \
                gzip.open(pjoin(curpath, 'url.gz'), 'r') as u:

            # Each document in a line base64 encoded
            # propagate url and collection for each document line
            # tab-separated text compressed, splitted in batchs of SIZE
            for i, doc in enumerate(p):
                # batch completed, create new one
                if n_chars >= args.size:
                    batch += 1
                    if ofp is not None:
                        ofp.close()

                    ofp = zstandard.open(
                            pjoin(args.output_dir, args.lang, f'batch.{batch}.zst'),
                            'wt', encoding='utf-8', cctx=cctx)
                    n_chars = 0

                # read b64 encoded documents and their urls
                try:
                    docurl = u.readline().strip() \
                                .decode('utf-8', errors='strict')
                    lines = base64.b64decode(doc.strip()) \
                                .decode('utf-8', errors='strict').split('\n')
                except UnicodeDecodeError:
                    print("Unicode error in doc {i} collection {coll} w2t batch {dirnum}",
                            file=sys.stderr)

                # Print each document with its url and collection
                for line in lines:
                    if line:
                        n_chars += len(line)
                        print(docurl, line, coll, sep='\t', file=ofp)
