#!/usr/bin/python
import sys
count = 0
for line in sys.stdin:
	if count % 2 == 0:
		sys.stdout.write("%s" % line)
	count = count + 1

