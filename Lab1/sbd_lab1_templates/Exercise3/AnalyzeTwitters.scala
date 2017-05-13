/* Author: Hamid Mushtaq (TU Delft) */
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io._
import java.util.Locale
import org.apache.commons.lang3.StringUtils

object AnalyzeTwitters
{
	// Gets Language's name from its code
	def getLangName(code: String) : String =
	{
		return new Locale(code).getDisplayLanguage(Locale.ENGLISH)
	}
	
	def main(args: Array[String]) 
	{
		val inputFile = args(0)
		val conf = new SparkConf().setAppName("AnalyzeTwitters")
		val sc = new SparkContext(conf)
		
		// Comment these two lines if you want more verbose messages from Spark
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		val t0 = System.currentTimeMillis
		
		// Add your code here
		
		// outRDD would have elements of type String.
		// val outRDD = ...
		// Write output
		/*val bw = new BufferedWriter(new OutputStreamWriter(System.out, "UTF-8"))
		bw.write("Language,Language-code,TotalRetweetsInThatLang,IDOfTweet,RetweetCount,Text\n")
		outRDD.collect.foreach(x => bw.write(x))
		bw.close*/
		
		val et = (System.currentTimeMillis - t0) / 1000
		System.err.println("Done!\nTime taken = %d mins %d secs".format(et / 60, et % 60))
	}
}

