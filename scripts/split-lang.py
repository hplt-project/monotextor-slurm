import zstandard
import orjson
import sys
import re
import os
import io

SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))
# Load Openlid to HPLT language code mappings
LANG_MAPPING = {}
with open(f"{SCRIPT_PATH}/openlidv2_to_hplt_codes.jsonl") as f:
    for line in f:
        lang_map = orjson.loads(line)
        LANG_MAPPING[lang_map["flores_code"]] = lang_map

# Obtain list of final HPLT lang codes
LANGS = [i["hplt_canonical_label"]+"_"+i["iso15924"] for i in LANG_MAPPING.values()]

THREADS = 10 if "SLURM_CPUS_ON_NODE" not in os.environ else os.environ["SLURM_CPUS_ON_NODE"]

class LangWriter():
    MAX_SIZE = 1e11 # max bytes per batch

    def __init__(self, directory):
        self.dir = directory
        self.bytes_written = 0
        self.num_files = 0
        try:
            os.mkdir(self.dir)
            # create dir, ignore error if exists
        except FileExistsError:
            pass
        self.compressor = zstandard.ZstdCompressor(level=10, threads=10)
        self.writer = None
        self.new_split()

    def new_split(self):
        if self.writer:
            self.writer.close()
        self.bytes_written = 0
        self.num_files += 1
        self.writer = zstandard.open(
                f"{self.dir}/batch_{self.num_files}.jsonl.zst",
                'wb',
                cctx=self.compressor)

    # Write bytes to the shard, if max size is reached, open a new batch
    def write(self, text_bytes):
        if self.bytes_written >= self.MAX_SIZE:
            self.new_split()
        self.writer.write(text_bytes)
        self.bytes_written += len(text_bytes)

    def close(self):
        self.writer.close()

    def __del__(self):
        self.writer.close()

output_dir = sys.argv[1]
input_files = sys.argv[2:]
print(output_dir)
print(input_files)
# compile byte-based regex
lang_re = re.compile(b'"lang": ?\["([a-z]{3}_[A-Z][a-z]{3})",.*\], ?"prob": ?\[([0-9]+\.[0-9]+),[0-9]+\.[0-9]+,[0-9]+\.[0-9]+\],')

# Create all the lang directories
# right now, the langcodes are hardcoded, but it could be a dynamic list
# that creates a new langrwiter every time a new lang is found
lang_files = {'unk': LangWriter(f'{output_dir}/unk')}
for lang in LANGS:
    cur_dir = f"{output_dir}/{lang}"
    lang_files[lang] = LangWriter(cur_dir)

for infile in input_files:
    with zstandard.open(infile, 'rb') as docs_file:
        for i, line in enumerate(io.BufferedReader(docs_file)):
            # obtain lang without decoding string nor parsing json
            match = lang_re.search(line)
            if match is None:
                print(line.decode('utf-8'), file=sys.stderr)
            try:
                # extract lang code and probability from the capture groups
                lang = match.groups()[0].decode()
                prob = float(match.groups()[1])
            except ValueError as e:
                # in case float cannot be parsed show what regex has captured
                print(line, file=sys.stderr)
                print(match, file=sys.stderr)
                raise e

            # confidence threshold
            if prob < 0.5:
                lang_files['unk'].write(line)
                continue

            # Map language to HPLT lang code
            lang_map = LANG_MAPPING[lang]
            lang = lang_map["hplt_canonical_label"] + "_" + lang_map["iso15924"]

            # move document to its lang dir
            lang_files[lang].write(line)

# close all files
for f in lang_files.values():
    f.close()
