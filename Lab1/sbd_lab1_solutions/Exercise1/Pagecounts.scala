import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io.File
import java.util.Locale
import org.apache.commons.lang3.StringUtils

object Pagecounts 
{
	def getLangName(code: String) : String =
	{
		new Locale(code).getDisplayLanguage(Locale.ENGLISH)
	}
	
	def write2File(content: Array[String], filePath: String)
	{
		new File("output").mkdirs
		val pw = new java.io.PrintWriter(new File(filePath))
		try {
			pw.write("Language, Language-code, TotalViews, MostVisitedPage, MostVisitedPage_Views\n")
			content.map(line => pw.write(line + "\n"))
		}
		finally { pw.close }
	}
	
	def main(args: Array[String]) 
	{
		val inputFile = args(0)
		val conf = new SparkConf().setAppName("Pagecounts")
		val sc = new SparkContext(conf)
		
		// Comment these two lines if you want a more verbose output
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		val t0 = System.currentTimeMillis
		
		// lang title views size
		val pageCounts = sc.textFile("file:///" + inputFile, 16)
		// (lang, title, views, size)
		val x1 = pageCounts.map(_.split(" "))
		// (langCode, (title, views))
		val x2 = x1.map(x => (StringUtils.substringBefore(x(0), "."), (x(1), x(2).toLong))).filter(x => x._1 != x._2._1)
		x2.cache()
		// (langCode, views)
		val lang2Views = x2.map{case(lang, info) => (lang, info._2)}//.filter(x => !(x._2 >= 0))
		// (langCode, totalViews) 
		val lang2TotalViews = lang2Views.reduceByKey(_ + _)
		// (langCode, (title, Views))
		val lang2MaxViews = x2.reduceByKey((a, b) => {if (b._2 > a._2) b else a})
		// (langCode, (totalViews, (mv_title, mv_views)))
		val y1 = lang2TotalViews.join(lang2MaxViews).sortBy(_._2._1, false)
		// (String)
		val csv = y1.map{case(langCode, (totalViews, (mv_title, mv_hits))) => 
			getLangName(langCode) + ", " + langCode + ", " + totalViews + ", \"" + mv_title + "\", " + mv_hits}
		write2File(csv.collect, "output/" + new File(inputFile).getName + ".csv")
		
		val et = (System.currentTimeMillis - t0) / 1000
		println("Done!\nTime taken = %d mins %d secs".format(et / 60, et % 60))
	}
}

