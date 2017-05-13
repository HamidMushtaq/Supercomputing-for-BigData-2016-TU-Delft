import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level

import java.io._
import java.nio.file.{Paths, Files}

object FastqChunker 
{
def main(args: Array[String]) 
{
	if (args.size < 3)
	{
		println("Not enough arguments!\nArg1 = number of parallel tasks = number of chunks\nArg2 = input folder\nArg3 = output folder")
		System.exit(1)
	}
	
	val prllTasks = args(0)
	val inputFolder = args(1)
	val outputFolder = args(2)
	
	if (!Files.exists(Paths.get(inputFolder)))
	{
		println("Input folder " + inputFolder + " doesn't exist!")
		System.exit(1)
	}
		 
	// Create output folder if it doesn't already exist
	new File(outputFolder).mkdirs
	
	println("Number of parallel tasks = number of chunks = " + prllTasks + "\nInput folder = " + inputFolder + "\nOutput folder = " + outputFolder)
	
	val conf = new SparkConf().setAppName("DNASeqAnalyzer")
	conf.setMaster("local[" + prllTasks + "]")
	conf.set("spark.cores.max", prllTasks)
	
	val sc = new SparkContext(conf)
	
	// Comment these two lines if you want to see more verbose messages from Spark
	Logger.getLogger("org").setLevel(Level.OFF);
	Logger.getLogger("akka").setLevel(Level.OFF);
		
	var t0 = System.currentTimeMillis
	
	// Rest of the code goes here
	
	val et = (System.currentTimeMillis - t0) / 1000 
	println("|Execution time: %d mins %d secs|".format(et/60, et%60))
}
//////////////////////////////////////////////////////////////////////////////
} // End of Class definition
