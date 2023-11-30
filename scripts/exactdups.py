from xxhash import xxh64_intdigest
import sys

hashtable = set()
for line in sys.stdin:
    digest = xxh64_intdigest(line.rstrip())

    if digest in hashtable:
        sys.stderr.write("Found duplicate!\n")
        sys.exit(1)

    hashtable.add(digest)

sys.stderr.write("No duplicates found\n")
