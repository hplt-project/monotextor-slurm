import zstandard
import orjson
import sys
import io

collection = sys.argv[1]
input_dir = sys.argv[2]

with zstandard.open(f'{input_dir}/text.zst', 'rt', errors='strict') as text_file, \
        zstandard.open(f'{input_dir}/metadata.zst', 'rb', errors='strict') as meta_file, \
        zstandard.open(f'{input_dir}/lang.zst', 'rt', errors='strict') as lang_file:

    for i, line_bytes in enumerate(io.BufferedReader(meta_file)):
        try:
            line = line_bytes.decode('utf-8')
        except UnicodeDecodeError:
            print(f"WARNING: discarded document with encoding error in metadata. Line {i+1} in file {input_dir}",
                  file=sys.stderr)
            text_file.readline()
            lang_file.readline()
            continue
        doc = orjson.loads(line)
        text = orjson.loads(text_file.readline())
        lang = orjson.loads(lang_file.readline())
        if not lang["lang"] or not text["t"] or lang["prob"][0]<=0.5:
            continue # remove empty docs or language

        doc["crawl_id"] = collection
        doc.update(lang)
        doc["text"] = text["t"] #insert the text at the end of the json
        if "x" in text:
            doc["xml"] = text["x"]
        if "htmllang" in text:
            doc["html_lang"] = text["htmllang"]

        # write bytes directly
        sys.stdout.buffer.write(orjson.dumps(doc, option=orjson.OPT_APPEND_NEWLINE))
