import zstandard
import orjson
import sys

input_dir = sys.argv[1]
output_dir = sys.argv[2]
level = 10
# doing compression in separated threads is much faster
compressor = zstandard.ZstdCompressor(level=level, threads=4)

with zstandard.open(f'{input_dir}/text.zst', 'rt', errors='strict') as text_file, \
        zstandard.open(f'{output_dir}/docs.zst', 'wb', cctx=compressor) as out_file, \
        zstandard.open(f'{input_dir}/metadata.zst', 'rt', errors='strict') as meta_file, \
        zstandard.open(f'{input_dir}/lang.zst', 'rt', errors='strict') as lang_file:
    for line in meta_file:
        doc = orjson.loads(line)
        text = orjson.loads(text_file.readline())
        lang = orjson.loads(lang_file.readline())
        if not lang["lang"] or not text["t"]:
            continue # remove empty docs or language

        doc.update(lang)
        doc["text"] = text["t"] #insert the text at the end of the json

        # write bytes directly
        out_file.write(orjson.dumps(doc))
        out_file.write(b'\n')

