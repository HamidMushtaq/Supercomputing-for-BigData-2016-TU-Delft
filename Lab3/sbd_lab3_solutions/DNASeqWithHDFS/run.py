#!/usr/bin/python
from xml.dom import minidom
import sys
import os
import time

# numInstances and numThreads would be required for cluster mode
doc = minidom.parse("./config.xml")
numInstances = doc.getElementsByTagName("numInstances")[0].firstChild.data
numThreads = doc.getElementsByTagName("numThreads")[0].firstChild.data
	
def run():
	cmdStr = "$SPARK_HOME/bin/spark-submit --jars lib/htsjdk-1.143.jar " + \
	"--class \"DNASeqAnalyzer\" --master yarn-cluster --files ./config.xml,./ucsc.hg19.dict " + \
	"--driver-memory 12g --executor-memory 32g " + \
	"--num-executors " + numInstances + " --executor-cores 2 target/scala-2.11/dnaseqanalyzer_2.11-1.0.jar"
	
	print cmdStr
	os.system(cmdStr)
	
run()
	
