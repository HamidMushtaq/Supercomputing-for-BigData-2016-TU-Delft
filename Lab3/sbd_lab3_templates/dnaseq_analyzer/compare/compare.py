#!/usr/bin/python
import sys

def getLineData( line ):
	s = line.split()
	if s[0].find('chr') != -1:
		return s[0] + "," + s[1] + "," + s[3] + "," + s[4]
	else:
		return ""
		
if len(sys.argv) < 3:
	print "\nInsufficient number of arguments!"
	print "Usage: python compare.py ref.vcf your.vcf\n"
	sys.exit(1)
	
f1 = sys.argv[1]
f2 = sys.argv[2]

f1data = {}
f2data = {}

with open(f1) as f:
	lines = f.readlines()
	for line in lines:
		line_data = getLineData(line)
		if line_data != "":
			f1data[line_data] = 0

with open(f2) as f:
	lines = f.readlines()
	for line in lines:
		line_data = getLineData(line)
		if (line_data != "") and (line_data in f1data):
			f2data[line_data] = 0

print "total = " + str(len(f1data))
print "matches = " + str(len(f2data))
print "diff = " + str(len(f1data) - len(f2data))