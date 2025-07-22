from argparse import ArgumentParser
import contextlib
import sys
import re
import os

from bifixer import restorative_cleaning
from xxhash import xxh128_hexdigest
from pii_manager import PiiEnum
from pii_manager.api import PiiManager
from pii_manager.lang import COUNTRY_ANY
from docscorer import DocumentScorer
from marisa_trie import Trie
from iso639 import Lang
import orjson
import regex

realpath = os.path.dirname(os.path.realpath(__file__))

parser = ArgumentParser()
parser.add_argument('lang', help='Target language')
parser.add_argument('-a','--all', action='store_true', help="Use all filters")
parser.add_argument('-e','--explicit', type=str, help="Remove explicit content with domain adult content list")
parser.add_argument('-E','--extended_explicit', action='store_true', help="Extended explicit url block looking for banned patterns")
parser.add_argument('-w','--avg_words', action='store_true', help="Remove docs that do not meet the minimum word average per segment")
parser.add_argument('-m','--minimum', action='store_true', help="Remove docs that do not meet the minimum size")
parser.add_argument('-l','--language', action='store_true', help="Remove docs that do not meet the minimum correct language pct")
parser.add_argument('-r','--robots', type=str, required=False, help="List of robots.txt disallowed urls")
parser.add_argument('-z','--cjk', action='store_true', help="Process CJK language")

args = parser.parse_args()
if args.all:
    args.avg_words = True
    args.extended_explicit = True
    args.minimum = True
    args.language = True

if args.lang.split('_')[0] in ('jpn','kor','yue','zho'):
    args.cjk = True

isolang = Lang(args.lang.split('_')[0])
#print(isolang, file=sys.stderr)

#print(sys.argv, file=sys.stderr)

url_prefix_re = regex.compile("^(https?:\/\/)?(www\.)?(.+)", regex.I)
extract_domain = regex.compile("^(?:https?:\/\/)?(?:[^@\/\n]+@)?(?:www\.)?([^:\/\n]+)(.*)", regex.I)
remove_subdomain = re.compile(".*?\.")
scorer = DocumentScorer()

# Load monofixer replacements
# if no pt1 it doesn't matter, monofixer does not support languages without pt1
monofixer_lang = 'any'
if args.lang.split('_')[1] in ('Hans' or 'Hant'):
    monofixer_lang = 'zh'
elif isolang.pt1:
    monofixer_lang = isolang.pt1
chars_lang, charsRe_lang = restorative_cleaning.getCharsReplacements(monofixer_lang)

@contextlib.contextmanager
def stdout_to_err():
    save_stdout = sys.stdout
    sys.stdout = sys.stderr
    yield
    sys.stdout = save_stdout

#PII regex
if isolang.pt3 == 'hbs':
    piilang = 'hbs'
elif not isolang.pt1:
    piilang = 'any'
else:
    piilang = isolang.pt1
country = COUNTRY_ANY
tasklist = (PiiEnum.IP_ADDRESS, PiiEnum.EMAIL_ADDRESS, PiiEnum.PHONE_NUMBER)
with stdout_to_err():
    proc = PiiManager(piilang, country, tasks=tasklist, mode="extract")

MIN_LENGTH = 500
MIN_LANG_RATIO = 0.2
MIN_AVG_WORDS = 5
MIN_AVG_CHARS = 10
BLOCKED_PATTERNS = ('porn', 'sex', 'tube', 'cams', 'camgirls', 'mature')

# Load adult domains
# create the trie with an iterator over the file to avoid loading all the file into memory
def file_iterator(filename):
    with open(filename, 'rt') as f:
        for line in f:
            yield line.strip()
adult_doms = None
if args.explicit:
    adult_doms = Trie(file_iterator(args.explicit))

# Load robotstxt disallowed
if args.robots:
    robots_urls = Trie(file_iterator(args.robots))

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

    # Average and median words per segment
    if args.cjk:
        words_dist = [len(i) for i in segs]
    else:
        words_dist = [len(i.split(' ')) for i in segs]
    avg_seg_words = sum(words_dist) / len(segs)

    # LM scores and langid means
    # split lang by underscore to discard possible script suffix
    # n_segs = len(segs)
    # if 'seg_langs' in doc and doc['seg_langs']:
    #     avg_correct_lang = sum(1 for l in doc['seg_langs'] if l.split('_')[0] == doc['lang'][0]) / n_segs
    # else:
    #     # If there is no langs field and correct lang is requested, please crash
    #     avg_correct_lang = None

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

# Look for PII, return matched ranges
def pii_multi(text):
    matches = proc(text)
    return list(sorted((i.pos, i.pos + len(i.value)) for i in matches))

# Apply character fixing and remove html tax by monofixer
# do it for each segment separatedly because monofixer removes endlines
# it is also the wey we've been applying monofixer until now
def monofixer(text):
    fixed_text = []
    for segment in text.split('\n'):
        if len(segment) >= 50000:
            fixed_text.append(segment)
            continue
        fixed_seg = restorative_cleaning.fix(segment, monofixer_lang, chars_lang, charsRe_lang)
        fixed_seg = restorative_cleaning.remove_html_tags(fixed_seg)
        fixed_text.append(fixed_seg)
    return '\n'.join(fixed_text)

# Check if the url is in the robots.txt disallowed list
def robots_filter(url):
    # remove the prefix of the url, same preprocessing as in robotstxt extraction
    url_noprefix = url_prefix_re.sub(r'\3', url)
    if url_noprefix in robots_urls:
        return "disallow"
    else:
        return "allow"

# Convert certain langcodes to a form that WDS supports
# because WDS may have the macro or the individual and openlidv2 otherwise
# so we try to match 'hplt_canonical_label' in
# https://huggingface.co/datasets/laurievb/OpenLID-v2/blob/cc7b18c/hplt/hplt_200_lang_labels.jsonl
# to language_3_chars here
# https://github.com/pablop16n/web-docs-scorer/blob/37a3f3421cc7495b9d7cb5bdd2ba36dad7e7db5d/src/docscorer/configurations/language_adaption/medians_language.csv
# if languages are not present in that csv, WDS will use standard generic values
# this function assumes input is a code from hplt_canonical_labels
def get_lang_wds(langcode_script):
    if langcode_script == 'und' or langcode_script == 'unk':
        return langcode_script
    try:
        langcode, script = langcode_script.split('_')
    except ValueError as e:
        raise ValueError(f"Could not parse {lancode_script}") from e
    if langcode == 'cmn':
        # openlidv2 uses individual code for mandarin
        return f'zho_{script}'
    if langcode_script == 'zsm_Latn':
        # wds only has 'ind' and 'msa'
        return 'msa_Latn'
    if langcode_script == 'swa_Latn':
        # wds has swahili individual
        return 'swh_Latn'
    if langcode_script == 'ara_Arab':
        # wds uses individual
        return 'arb_Arab'
    if langcode in ('lvs', 'ltg'):
        return 'lav_Latn'

    return langcode_script

for line in sys.stdin:
    doc = orjson.loads(line)
    doc["id"] = xxh128_hexdigest(doc["f"] + doc["u"] + doc["ts"])
    doc['text'] = monofixer(doc['text'])
    doc["filter"] = filter_doc(args, doc)
    doc["pii"] = pii_multi(doc["text"])
    if args.robots:
        doc["robots"] = robots_filter(doc["u"])
    wds_seg_langs = list(map(get_lang_wds, doc["seg_langs"]))
    doc["doc_scores"] = scorer.score_text(
            ref_lang=get_lang_wds(args.lang), # we use args.lang because it has been converted to hplt codes
            lang_segments=wds_seg_langs,
            scores_lang=[1.0]*len(doc["seg_langs"]), #TODO hack, should remove this
            document_text=doc["text"],
            id=doc["id"],
            script_sys=doc["lang"][0].split("_")[1],
            raw_score=False,
            )

    sys.stdout.buffer.write(orjson.dumps(doc, option=orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_APPEND_NEWLINE))
