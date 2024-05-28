import sys

sys.stdin.reconfigure(errors='replace')
try:
    for i, line in enumerate(sys.stdin):
        print(line.strip())
except UnicodeDecodeError as e:
    print(f"ERROR in line {i+2}", file=sys.stdout)
    raise e
