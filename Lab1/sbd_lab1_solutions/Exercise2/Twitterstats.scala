/* Author: Hamid Mushtaq (TU Delft) */
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import java.io._
import java.util.Locale
import java.text._
import java.net._
import java.util.Calendar
import org.apache.tika.language.LanguageIdentifier
import java.util.regex.Matcher
import java.util.regex.Pattern
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.parsers.DocumentBuilder
import org.w3c.dom.Document

//https://github.com/apache/bahir/blob/master/streaming-twitter/examples/src/main/scala/org/apache/spark/examples/streaming/twitter/TwitterPopularTags.scala

object Twitterstats
{ 
	var firstTime = true
	var t0: Long = 0
	val pw = new java.io.PrintWriter(new File("twitterLog.csv"))
	
	// (lang, (totalCount, (id, text, max_count, min_count)))
	def printTweets(tweets: Array[(String, ((Long), (Long, String, Long, Long)))])
	{
		for( i <- 0 until tweets.size)
		{
			val lang = getLangNameFromCode(tweets(i)._1)
			val totalCount = tweets(i)._2._1
			val value = tweets(i)._2._2
			val diff = if (value._3 == value._4) 1 else (value._3 - value._4)
		
			println(i + ". " + lang + " -> totalCount = " + totalCount + ", " + value._2 + ", " + value._3 + " - " + value._4 + " = " + diff)
		}
		println("\n-----------------------------------\n")
	}
	
	def write2Log(tweets: Array[(String, ((Int), (Long, String, Int, Int)))])
	{
		if (firstTime)
		{
			pw.write("Seconds,Language,Language-code,TotalLangCount,ID,MaxCount,MinCount,Count,Text\n")
			t0 = System.currentTimeMillis
			firstTime = false
		}
		else
		{
			val seconds = (System.currentTimeMillis - t0) / 1000
			
			if (seconds < 60)
			{
				println("\nElapsed time = " + seconds + " seconds. Logging will be started after 60 seconds.")
				return
			}
			
			println("Logging the output to the log file\nElapsed time = " + seconds + " seconds\n-----------------------------------")
			
			for(i <-0 until tweets.size)
			{
				val lang = getLangNameFromCode(tweets(i)._1)
				val totalLangCount = tweets(i)._2._1
				val value = tweets(i)._2._2
				val count = if (value._3 == value._4) 1 else (value._3 - value._4)
				val textStr = value._2.replaceAll("\\r|\\n", " ")
				
				// We enclose seconds with brackets because otherwise languages like persian would write seconds in reverse!
				pw.write("(" + seconds + ")," + lang + "," + tweets(i)._1 + "," + totalLangCount + "," + value._1 + "," + 
					value._3 + "," + value._4 + "," + count + "," + textStr + "\n")
			}
		}
	}
	
	// (lang, TotalCount)
	def printTweets1(tweets: Array[(String, Long)])
	{
		for( i <- 0 until tweets.size)
		{
			val lang = getLangNameFromCode(tweets(i)._1)
			val totalCounts = tweets(i)._2
		
			println(i + ". " + lang + " -> " + totalCounts)
		}
		println("\n-----------------------------------\n")
	}
			
	// (lang, (id, text, max_count, min_count))
	def printTweets2(tweets: Array[(String, (Long, String, Long, Long))])
	{
		for( i <- 0 until tweets.size)
		{
			val lang = getLangNameFromCode(tweets(i)._1)
			val value = tweets(i)._2
			val diff = if (value._3 == value._4) 1 else (value._3 - value._4)
		
			println(i + ". " + lang + " -> " + value._2 + ", " + value._3 + " - " + value._4 + " = " + diff)
		}
		println("\n-----------------------------------\n")
	}
  
	def getLang(s: String) : String =
	{
		val inputStr = s.replaceFirst("RT", "").replaceAll("@\\p{L}+", "").replaceAll("https?://\\S+\\s?", "")
		var langCode = new LanguageIdentifier(inputStr).getLanguage
		
		// Detect if japanese
		var pat = Pattern.compile("\\p{InHiragana}") 
		var m = pat.matcher(inputStr)
		if (langCode == "lt" && m.find)
			langCode = "ja"
		// Detect if korean
		pat = Pattern.compile("\\p{IsHangul}");
		m = pat.matcher(inputStr)
		if (langCode == "lt" && m.find)
			langCode = "ko"
		
		return langCode
	}
  
	def getLangNameFromCode(code: String) : String =
	{
		return new Locale(code).getDisplayLanguage(Locale.ENGLISH)
	}
  
	def main(args: Array[String]) 
	{
		val file = new File("cred.xml")
		val documentBuilderFactory = DocumentBuilderFactory.newInstance
		val documentBuilder = documentBuilderFactory.newDocumentBuilder
		val document = documentBuilder.parse(file);
			
		// Configure Twitter credentials
		val consumerKey = document.getElementsByTagName("consumerKey").item(0).getTextContent 				
		val consumerSecret = document.getElementsByTagName("consumerSecret").item(0).getTextContent 		
		val accessToken = document.getElementsByTagName("accessToken").item(0).getTextContent 				
		val accessTokenSecret = document.getElementsByTagName("accessTokenSecret").item(0).getTextContent	
		
		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF)
		Logger.getRootLogger.setLevel(Level.OFF)

		// Set the system properties so that Twitter4j library used by twitter stream
		// can use them to generate OAuth credentials
		System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
		System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
		System.setProperty("twitter4j.oauth.accessToken", accessToken)
		System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

		val sparkConf = new SparkConf().setAppName("TwitterPopularTags")

		val ssc = new StreamingContext(sparkConf, Seconds(1))
		val tweets = TwitterUtils.createStream(ssc, None)
		val retweetedStatuses = tweets.filter(status => status.isRetweet)// && getLang(status.getText) == "en")
		//////////////////////////////////////////////////////////////////////////
		val statuses = retweetedStatuses.map(status => (status.getRetweetedStatus.getId(), 
			(status.getRetweetedStatus.getRetweetCount,
			status.getRetweetedStatus.getRetweetCount,
			getLang(status.getText),
			status.getText)))
		val statusesSorted = statuses.transform(rdd => rdd.sortByKey())
	 
		val counts = statusesSorted.reduceByKeyAndWindow((a:(Int, Int, String, String), b:(Int, Int, String, String)) => 
			(math.max(a._2, b._2), math.min(a._2, b._2), a._3, a._4), Seconds(60), Seconds(5))
		// (lang, (id, text, max_count, min_count))
		val ds = counts.map{case(id, (max_count, min_count, lang, text)) => (lang, (id, text, max_count, min_count))}
		ds.foreachRDD(rdd => rdd.cache())
		// (lang, totalCount)
		val x1 = ds.map(x => (x._1, (if (x._2._3 == x._2._4) 1 else (x._2._3 - x._2._4)))).transform(rdd => rdd.reduceByKey(_ + _))
		// (lang, (totalCount, (id, text, max_count, min_count)))
		val y = x1.join(ds).transform(rdd => rdd.sortBy(x => (x._2._1, x._2._2._3 - x._2._2._4), false))
		//////////////////////////////////////////////////////////////////////////				 
		y.foreachRDD(rdd => write2Log(rdd.collect))
	
		ssc.start()
		ssc.awaitTermination()
	}
}

