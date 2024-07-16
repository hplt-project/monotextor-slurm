import sys
import re

from unicodedata import category as cat
import zstandard
import orjson

robots = {}
domain_re = re.compile("(https?://)?(www.)?([^/]*)(/.*)")

allow_re = re.compile("((Dis)?Allow): ?(\/.*?)(#.*)?$", re.I)
blank_re = re.compile("^\s*$")
agent_re =  re.compile("^User-agent: ?([\*\.\\\-\_a-z]+?)(#.*)?$", re.I)
agents = ('ia_archiver', 'ccbot', '*')

tbl = [chr(i) for i in range(sys.maxunicode) if not cat(chr(i)).startswith('L') and chr(i) not in [',', '-','_', '*']]
clean_agent_chars = str.maketrans('', '', ''.join(tbl))

def parse(robotstext, domain):
    allowance = False
    for line in robotstext.split('\n'):
        if line.startswith('#'):
            continue # ignore comments

        agent_match = agent_re.match(line)
        # If our user-agents are found, do not ignore following entries
        if agent_match and agent_match.groups()[0] in agents:
            allowance = True
            continue

        # Parse entry for our agents
        if allowance:
            # blank means end of entries group
            if blank_re.match(line):
                allowance = False #In an endline comes, start new group
                continue
            # Look for allow or disallow
            # print allow entries with a 1, disallow with a 0
            allow_match = allow_re.match(line)
            if allow_match:
                url_pattern = f"{domain}{allow_match.groups()[2]}".strip().replace('\t','')
                if allow_match.groups()[0].lower() == "allow":
                    print(f"{url_pattern}\t1")
                elif allow_match.groups()[0].lower() == "disallow":
                    print(f"{url_pattern}\t0")

if __name__ == "__main__":
    for line in sys.stdin:
        doc = orjson.loads(line)

        # Skip robots that do not disallow anything or documents that are not robots.txt
        if doc["p"].lower().find('disallow:') == -1:
            continue

        domain = domain_re.sub(r"\3", doc["u"])
        parse(doc['p'], domain)
