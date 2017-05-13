#!/usr/bin/python
from xml.dom import minidom
import sys
import os
import time

# numInstances would be required for cluster mode
doc = minidom.parse("./config.xml")
numInstances = doc.getElementsByTagName("numInstances")[0].firstChild.data
	
def run():
	cmdStr = "$SPARK_HOME/bin/spark-submit --jars lib/htsjdk-1.143.jar " + \
	"--class \"DNASeqAnalyzer\" --master local[*] --driver-memory 32g target/scala-2.11/dnaseqanalyzer_2.11-1.0.jar"
	print cmdStr
	os.system(cmdStr)
	
run()
	