#!/usr/bin/env python3
from subprocess import Popen, PIPE, TimeoutExpired
from argparse import ArgumentParser
from os.path import join as pjoin
import binascii
import base64
import sys
import os


parser = ArgumentParser(description="Gather collection name, URL and text "
                        " from warc2text batches."
                        " Output a tab-separated file to stderr."
                        "\nFormat: url,paragraph,<paragraph_id>,collection")
parser.add_argument('input_dir', type=str,
                    help='warc2text subdirectory where text.gz and url.gz re stored')
args = parser.parse_args()


# Check decompress commands exist
from shutil import which
assert which('pigz') is not None
assert which('zcat') is not None

dir_parts = args.input_dir.rstrip('/').split('/')
coll = dir_parts[-3]
dirnum = dir_parts[-2]
print(f"Reading {coll}/{dirnum}", file=sys.stderr)

# Read text and url files
p = Popen(['zcat', pjoin(args.input_dir, 'text.gz')],
          stdout = PIPE, stderr = PIPE)
u = Popen(['zcat', pjoin(args.input_dir, 'url.gz')],
          stdout = PIPE, stderr = PIPE)

# Each document in a line base64 encoded
# propagate url and collection for each document line
# tab-separated text compressed, splitted in batchs of SIZE
for i, doc in enumerate(p.stdout):
    # read b64 encoded documents and their urls
    try:
        docurl = u.stdout.readline().strip() \
                    .decode('utf-8', errors='strict')
        lines = base64.b64decode(doc.strip()) \
                    .decode('utf-8', errors='strict').split('\n')
    except (UnicodeDecodeError, binascii.Error):
        print(f"WARNING: unicode error in doc {i} of {args.input_dir}",
                file=sys.stderr)

    # Print each document with its url and collection
    for line in lines:
        if line:
            print(f'{docurl}\t{line}\t{coll}')

# Check child decompressing processes ended well
# otherwise print their stderr messages for easier debugging
try:
    p.wait(timeout=60)
    u.wait(timeout=60)
except TimeoutExpired:
    print(f"#### Command timed out {coll}/{dirnum} ####", file=sys.stderr)
finally:
    if p.returncode != 0 or u.returncode != 0:
        print(f"#### text.gz stderr {coll}/{dirnum} ####", file=sys.stderr)
        print(p.stderr.read(), file=sys.stderr)
        print(f"#### url.gz stderr {coll}/{dirnum} ####", file=sys.stderr)
        print(u.stderr.read(), file=sys.stderr)
        raise RuntimeError()
