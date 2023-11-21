from collections import Counter
import sys
import regex

filter_reason = regex.compile(r'\"filter\":\"(.+?)\"')

stats = Counter()
for i, line in enumerate(sys.stdin):
    reason = filter_reason.search(line.rstrip()).group(1)
    if reason is None:
        raise Exception(f"Error, could not match filter reason in line {i+1}")
    stats[reason] += 1

total = sum(stats.values())
for reason, count in sorted(stats.items()):
    print(f"{reason}\t{count/total*100:.2f}%\t{count}")
