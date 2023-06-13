#!/usr/bin/env python3
from argparse import ArgumentParser
import orjson
import sys

# Print a jsonl per document
# Each line (paragraph) is concatenated until
# end of document is reached (the url changes)

parser = ArgumentParser("Convert tsv to jsonl")
parser.add_argument('-c', '--columns', required=True,
                    help='Column names separated by comma,'
                         ' odered as they appear in the file')
parser.add_argument('-l', '--lang', required=True,
                    help='Language of the documents')
args = parser.parse_args()

fields = [n for n in args.columns.split(',')]

prev_url = None
doc_id = 0
jsonl = {'id': doc_id, 'document_lang': args.lang}
for line in sys.stdin:
    parts = line.rstrip('\n').split('\t')
    url = parts[0]

    # url has changed
    # current line is a different doc, print the previous doc
    if prev_url is not None and prev_url != url:
        print(orjson.dumps(jsonl).decode('utf-8'))
        doc_id += 1
        jsonl = {'id': doc_id, 'document_lang': args.lang}

    for i, field in enumerate(fields):
        if field == 'text':
            # Concatenate paragraphs with endline separators
            # to reconstruct the document
            if 'text' not in jsonl:
                jsonl['text'] = parts[i]
            else:
                jsonl[field] += '\n' + parts[i]
        elif field in ('url', 'collection'):
            # Assign url and collection only once
            # url delimits the doc, a doc can only be in one coll
            if field not in jsonl:
                jsonl[field] = parts[i]
        else:
            # Concatenate in a list the remaining metadata fields
            if field not in jsonl:
                jsonl[field] = [parts[i]]
            else:
                jsonl[field].append(parts[i])

        prev_url = url

if 'text' in jsonl:
    print(orjson.dumps(jsonl).decode('utf-8'))
