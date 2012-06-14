#!/usr/bin/python
import sys
if len(sys.argv) != 2:
	sys.exit(1);
delim=sys.argv[1];	
for line in sys.stdin.readlines():
	line = line.strip();
	line = line.replace('|',delim);
	sys.stdout.write("streaming\t%s\n" % line)
