from itertools import islice

with open("file") as f:
    for line in islice(f, n):
        print line