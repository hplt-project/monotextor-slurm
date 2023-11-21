import sys
import os

directory = os.environ["WORKSPACE"]
langs = sys.argv[1:]

# Read stats files into a dictionary
table = {i: {} for i in langs}
for l in langs:
    stats_file = f"{directory}/clean/{l}/{l}_stats"

    if os.path.exists(stats_file):
        with open(stats_file) as f:
            for line in f:
                parts = line.strip().split('\t')
                table[l][parts[0]] = parts[1]

table_cols = set()
for rows in table.values():
    table_cols.update(rows.keys())
table_cols.remove("keep")

print("lang\tkeep", end='')
for c in sorted(table_cols):
    print('\t', end='')
    print(c, end='')
print()

table_cols = sorted(table_cols)
# print the columns for each language, print keep first
for l in langs:
    print(l, end='\t')

    if "keep" not in table[l]:
        print(l, table[l], file=sys.stderr)
    if len(table[l]) > 0:
        print(table[l]["keep"], end='')
    else:
        print()
        continue

    for c in table_cols:
        print('\t', end='')
        if c in table[l]:
            print(table[l][c], end='')
        else:
            print("", end='')
    print()
