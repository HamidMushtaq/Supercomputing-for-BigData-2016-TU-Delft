/* Bacon.scala */
/* Author: Hamid Mushtaq */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level

import java.io._
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.Calendar

object Bacon 
{
	final val KevinBacon = "Bacon, Kevin (I)"	// This is how Kevin Bacon's name appears in the input file for male actors
	val compressRDDs = false
	// SparkListener must log its output in file sparkLog.txt
	val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("sparkLog.txt"), "UTF-8"))
	
	def getTimeStamp() : String =
	{
		return "[" + new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime()) + "] "
	}
	
	def main(args: Array[String]) 
	{
		val cores = args(0)				// Number of cores to use
		val inputFileM = args(1)		// Path of input file for male actors
		val inputFileF = args(2)		// Path of input file for female actors (actresses)
		
		val conf = new SparkConf().setAppName("Kevin Bacon app")
		conf.setMaster("local[" + cores + "]")
		conf.set("spark.cores.max", cores)
		val sc = new SparkContext(conf)
		
		// Add SparkListener's code here
		
		println("Number of cores: " + args(0))
		println("Input files: " + inputFileM + " and " + inputFileF)
		
		// Comment these two lines if you want to see more verbose messages from Spark
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		var t0 = System.currentTimeMillis
		
		// Add your main code here
		
		sc.stop()
		bw.close()
		
		val et = (System.currentTimeMillis - t0) / 1000 
		println("{Time taken = %d mins %d secs}".format(et/60, et%60))
	} 
}
