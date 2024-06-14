from argparse import ArgumentParser
from collections import Counter
import random
import sys
import re
import os

from unicodedata import category as cat
from docscorer import DocumentScorer
import fasttext
import orjson
import regex
fasttext.FastText.eprint = lambda x: None

realpath = os.path.dirname(os.path.realpath(__file__))

parser = ArgumentParser()
parser.add_argument('-a','--all', action='store_true', help="Use all filters")
parser.add_argument('-e','--explicit', action='store_true', help="Remove explicit content with UT1 adult list")
parser.add_argument('-E','--extended_explicit', action='store_true', help="Extended explicit url block looking for banned patterns")
parser.add_argument('-w','--avg_words', action='store_true', help="Remove docs that do not meet the minimum word average per segment")
parser.add_argument('-m','--minimum', action='store_true', help="Remove docs that do not meet the minimum size")
parser.add_argument('-l','--language', action='store_true', help="Remove docs that do not meet the minimum correct language pct")
parser.add_argument('-z','--cjk', action='store_true', help="Process CJK language")

args = parser.parse_args()
if args.all:
    args.explicit = True
    args.avg_words = True
    args.minimum = True
    args.language = True

#print(sys.argv, file=sys.stderr)

extract_domain = regex.compile("^(?:https?:\/\/)?(?:[^@\/\n]+@)?(?:www\.)?([^:\/\n]+)(.*)", regex.I)
remove_subdomain = re.compile(".*?\.")
scorer = DocumentScorer()
langid = fasttext.load_model('lid193_merged_arabics.bin')
# from https://github.com/hplt-project/warc2text-runner/blob/67f708dad8c5943701d591751a6e7a787e3e65bc/src/warc2text_runner/two/fastertext_lid/patterns.py
nonword_regex = regex.compile(r"[^\p{Word}\p{Zs}]|\d")

MIN_LENGTH = 200
MIN_LANG_RATIO = 0.2
MIN_AVG_WORDS = 5
MIN_AVG_CHARS = 10
BLOCKED_PATTERNS = ('porn', 'sex', 'tube', 'cams', 'camgirls', 'mature')

# Load adult domains
with open(f'./blocklists/adult_domains') as f:
    adult_doms = set(i.strip() for i in f)

def is_adult(url, extended=False):
    domain = extract_domain.sub(r"\1", url)
    # We check removing subdomains
    # this may help match sites with language as a subdomain in the url
    # or other subdomains not included in the list
    # this should be safe, as the list won't contain things like just "blogspot.com" or just ".com"
    shorter1 = remove_subdomain.sub('', domain, count=1)
    shorter2 = remove_subdomain.sub('', domain, count=2)

    if domain in adult_doms or shorter1 in adult_doms or shorter2 in adult_doms:
        return True
    if extended and any(i in url for i in BLOCKED_PATTERNS):
        return True
    return False


def filter_doc(args, doc):
    text = doc['text']

    segs = text.split('\n')
    n_segs = len(segs)

    # Average and median words per segment
    if args.cjk:
        words_dist = [len(i) for i in segs]
    else:
        words_dist = [len(i.split(' ')) for i in segs]
    avg_seg_words = sum(words_dist) / len(segs)

    # LM scores and langid means
    # split lang by underscore to discard possible script suffix
    if 'seg_langs' in doc and doc['seg_langs']:
        avg_correct_lang = sum(1 for l in doc['seg_langs'] if l.split('_')[0] == doc['lang'][0]) / n_segs
    else:
        # If there is no langs field and correct lang is requested, please crash
        avg_correct_lang = None

    # Filter criteria
    if args.explicit and is_adult(doc['u'], args.extended_explicit):
        return "adult_ut1"

    if args.avg_words:
        if args.cjk and avg_seg_words <= MIN_AVG_CHARS:
            return f"char_avg_{MIN_AVG_CHARS}"
        if not args.cjk and avg_seg_words <= MIN_AVG_WORDS:
            return f"word_avg_{MIN_AVG_WORDS}"

    if args.minimum and len(text) <= MIN_LENGTH:
        return f"length_{MIN_LENGTH}"

    #if args.language and avg_correct_lang <= MIN_LANG_RATIO:
    #    return f"lang_ratio_{MIN_LANG_RATIO}"

    return "keep"

# perform language identification for wach segment in a document
def segment_langid(text):
    lang_segments = []
    for segment in text.split('\n'):
        # apply same preprocessing as lid193 + lowercase
        segment = nonword_regex.sub('', segment.lower())
        label = langid.predict(segment)
        pred = label[0][0].replace("__label__","")
        lang_segments.append(pred)
    return lang_segments

for line in sys.stdin:
    #TODO apply monofixer
    doc = orjson.loads(line)
    doc["seg_langs"] = segment_langid(doc["text"])
    #TODO sort out the langcodes matching
    # docscorer internal langstats need to be adapted to the new codes
    # some langs may need to be converted to the macro code before scoring
    doc["filter"] = filter_doc(args, doc)
    doc["doc_scores"] = scorer.score_text(
            ref_lang=doc["lang"][0],
            lang_segments=doc["seg_langs"],
            scores_lang=[1.0]*len(doc["seg_langs"]), #TODO hack, should remove this
            document=doc["text"],
            )

    print(orjson.dumps(doc, option=orjson.OPT_SERIALIZE_NUMPY).decode('utf-8'))
