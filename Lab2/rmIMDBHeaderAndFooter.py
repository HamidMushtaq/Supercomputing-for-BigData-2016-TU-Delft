#!/usr/bin/python
# Removes the header and footer of an IMDB file
# Created by Hamid Mushtaq	

import sys
import time
import os

if len(sys.argv) < 3:
	print("Not enough arguments!")
	print("Example usage: ./removeIMDBHeaderAndFooter.py actors.list new_actors.list")
	sys.exit(1)

if not os.path.isfile(sys.argv[1]):
	print("File " + sys.argv[1] + " does not exist!")
	sys.exit(1)

start_time = time.time()

with open(sys.argv[1]) as f:
	lines = f.readlines()
		
print 'There are ' + str(len(lines)) + ' lines in ' + sys.argv[1]
print 'Now writing to file ' + sys.argv[2] + ' after removing header and footer...'

start_writing = False
name_titles_line = 0
counter = 0

fout = open(sys.argv[2], 'w')
for line in lines:
	counter = counter + 1
	if (name_titles_line == 0) and (''.join(line.split()) == 'NameTitles'):
		name_titles_line = counter
	elif (name_titles_line != 0) and (counter == (name_titles_line + 1)):
		start_writing = True
	elif start_writing:
		if (len(line) > 3) and (line[0:3] == '---'):
			break
		fout.write(line)
fout.close()

time_in_secs = int(time.time() - start_time)
print "Done!\n|Time taken = " + str(time_in_secs / 60) + " mins " + str(time_in_secs % 60) + " secs|"
