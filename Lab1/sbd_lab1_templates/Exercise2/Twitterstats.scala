/* Author: Hamid Mushtaq (TU Delft) */
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import java.io._
import java.util.Locale
import org.apache.tika.language.LanguageIdentifier
import java.util.regex.Matcher
import java.util.regex.Pattern

object Twitterstats
{ 
	var firstTime = true
	var t0: Long = 0
	val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("twitterLog.txt"), "UTF-8"))
	
	// This function will be called periodically after each 5 seconds to log the output. 
	// Elements of a are of type (lang, totalRetweetsInThatLang, idOfOriginalTweet, text, maxRetweetCount, minRetweetCount)
	def write2Log(a: Array[(String, Long, Long, String, Long, Long)])
	{
		if (firstTime)
		{
			bw.write("Seconds,Language,Language-code,TotalRetweetsInThatLang,IDOfTweet,MaxRetweetCount,MinRetweetCount,RetweetCount,Text\n")
			t0 = System.currentTimeMillis
			firstTime = false
		}
		else
		{
			val seconds = (System.currentTimeMillis - t0) / 1000
			
			if (seconds < 60)
			{
				println("Elapsed time = " + seconds + " seconds. Logging will be started after 60 seconds.")
				return
			}
			
			println("Logging the output to the log file\nElapsed time = " + seconds + " seconds\n-----------------------------------")
			
			for(i <-0 until a.size)
			{
				val langCode = a(i)._1
				val lang = getLangName(langCode)
				val totalRetweetsInThatLang = a(i)._2
				val id = a(i)._3
				val textStr = a(i)._4.replaceAll("\\r|\\n", " ")
				val maxRetweetCount = a(i)._5
				val minRetweetCount = a(i)._6
				val retweetCount = maxRetweetCount - minRetweetCount + 1
				
				bw.write("(" + seconds + ")," + lang + "," + langCode + "," + totalRetweetsInThatLang + "," + id + "," + 
					maxRetweetCount + "," + minRetweetCount + "," + retweetCount + "," + textStr + "\n")
			}
		}
	}
  
	// Pass the text of the retweet to this function to get the Language (in two letter code form) of that text.
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
  
	// Gets Language's name from its code
	def getLangName(code: String) : String =
	{
		return new Locale(code).getDisplayLanguage(Locale.ENGLISH)
	}
  
	def main(args: Array[String]) 
	{
		// Configure Twitter credentials
		val apiKey = "..."
		val apiSecret = "..."
		val accessToken = "..."
		val accessTokenSecret = "..."
		
		Helper.configureTwitterCredentials(apiKey, apiSecret, accessToken, accessTokenSecret)
		
		val ssc = new StreamingContext(new SparkConf(), Seconds(5))
		val tweets = TwitterUtils.createStream(ssc, None)
		
		// Add your code here
		
		// If elements of RDDs of outDStream aren't of type (lang, totalRetweetsInThatLang, idOfOriginalTweet, text, maxRetweetCount, minRetweetCount),
		//	then you must change the write2Log function accordingly.
		//outDStream.foreachRDD(rdd => write2Log(rdd.collect))
	
		new java.io.File("cpdir").mkdirs
		ssc.checkpoint("cpdir")
		ssc.start()
		ssc.awaitTermination()
	}
}

