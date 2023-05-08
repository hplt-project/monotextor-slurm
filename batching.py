from argparse import ArgumentParser
from subprocess import Popen, PIPE
from shutil import which
import sys
import os
assert which('zstd') is not None

SIZE = 10 * (1024 ** 3)

parser = ArgumentParser(description="Split lines into batches zstdcompressed.")
parser.add_argument('-s', '--size', type=int, default=SIZE,
                    help='Size of batches in number of characters (approximated)')
parser.add_argument('output_prefix', type=str,
                    help='Output files prefix to create bathes.'
                         ' Will follow the structure {prefix}.{num}.zst')
args = parser.parse_args()

ofp = None
n_chars = args.size # set to size, trigger file creation in the first step
batch = 0 # batch 0 will never be created
for line in sys.stdin:
    # batch completed, create new one
    if n_chars >= args.size:
        batch += 1
        if ofp is not None:
            ofp.stdin.close()
            ofp.wait(timeout=60)
            os.rename(ofp_name, ofp_name.removesuffix('.tmp'))

        # Files are written to to temp, renamed when finished
        ofp_name = f'{args.output_prefix}.{batch}.zst.tmp'
        ofp = Popen(['zstd', '-fo', ofp_name], encoding='utf-8',
                    stdin=PIPE, stdout=PIPE, stderr=PIPE)
        n_chars = 0

    n_chars += len(line)
    try:
        ofp.stdin.write(line)
    except BrokenPipeError:
        print(ofp.stdout.read())
        print(ofp.stderr.read())
        sys.exit(1)

if ofp:
    ofp.stdin.close()
    os.rename(ofp_name, ofp_name.removesuffix('.tmp'))
