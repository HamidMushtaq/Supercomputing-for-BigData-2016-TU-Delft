/* Author: Hamid Mushtaq (TU Delft) */
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io.File
import java.util.Locale
import org.apache.commons.lang3.StringUtils

object AnalyzeTwitters
{
	def main(args: Array[String]) 
	{
		val inputFile = args(0)
		val conf = new SparkConf().setAppName("Pagecounts")
		val sc = new SparkContext(conf)
		
		// Comment these two lines if you want a more verbose output
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		val t0 = System.currentTimeMillis
		
		// Seconds, Language, Language-code, TotalLangCount, ID, MaxCount, MinCount, Count, Text
		val twitters = sc.textFile("file:///" + inputFile, 16).filter(x =>  x.split(',')(0) != "Seconds")
		// (ID, (MaxCount, MinCount, Language-code, Text))
		val twittersRDD = twitters.map(x => x.split(',')).filter(_.size >= 9).map(x => (x(4).toLong, (x(5).toLong, x(6).toLong, x(2), x(8)))) 
		// (ID, (max MaxCount, min MinCount, Language-code, Text))
		val x1 = twittersRDD.reduceByKey((a, b) => (Math.max(a._1, b._1), Math.min(a._2, b._2), a._3, a._4))
		// (ID, (totalCount, Language-code, Text))
		val rdd = x1.map{case(id, values) => (id, (values._1 - values._2, values._3, values._4))}
		rdd.cache()
		// (Language-code, langTotalCount)
		val langsByTotalCount = rdd.map{case(id, values) => (values._2, values._1)}
		val y1 = langsByTotalCount.reduceByKey(_ + _)
		// (Language-code, (ID, totalCount, Text))
		val y2 = rdd.map{case(id, values) => (values._2, (id, values._1, values._3))}
		// (Language-code, (langTotalCount, (ID, totalCount, Text)))
		val y12 = y1.join(y2)
		// ((langTotalCount, totalCount), (ID, Language-code, Text))
		val y = y12.map{case(lc, (ltc, (id, tc, t))) => ((ltc, tc), (id, lc, t))}
		implicit val caseInsensitiveOrdering = new Ordering[(Long, Long)] {
			override def compare(a: (Long, Long), b: (Long, Long)) = if (a._1 == b._1) a._2.compare(b._2) else a._1.compare(b._1)
		}
		val finalRDD = y.sortByKey(false).filter(x => x._1._2 > 1)
		finalRDD.collect.foreach{x => println(x._2._2 + ", " + x._1._1 + ", " + x._1._2 + ", " + x._2._3)}
		
		//val x2 = x1.map{case(id, values) => (id, (values._1 - values._2, values._3, values._4))}//.sortBy(_._2._1, false)
		//x1.take(50).foreach(x => println(x._1 + ":" + x._2._1 + ":" + x._2._2 + ":" + x._2._3 + ":" + x._2._4))
		
		val et = (System.currentTimeMillis - t0) / 1000
		println("Done!\nTime taken = %d mins %d secs".format(et / 60, et % 60))
	}
}

