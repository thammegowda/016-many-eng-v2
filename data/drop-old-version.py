#!/usr/bin/env python
#
#
# Author: Thamme Gowda
# Created: 2/5/22
import sys

last = None
skipped = False
for line in sys.stdin:
    line = line.strip()
    row = line.split("\t")
    if last:
        if [last[x] for x in [1, 2, 4, 5]] == [row[x] for x in [1, 2, 4, 5]]:
            skipped = True
        else:
            print('\t'.join(last))
    else:
        skipped = True
    last = row
if skipped:
    print('\t'.join(last))


