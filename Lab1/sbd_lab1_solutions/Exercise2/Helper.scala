import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.mutable.HashMap

object Helper {
	Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
	Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)
  
	/** Configures the Oauth Credentials for accessing Twitter */
	def configureTwitterCredentials(apiKey: String, apiSecret: String, accessToken: String, accessTokenSecret: String) 
	{
		val configs = new HashMap[String, String] ++= Seq(
			"apiKey" -> apiKey, "apiSecret" -> apiSecret, "accessToken" -> accessToken, "accessTokenSecret" -> accessTokenSecret)
		println("Configuring Twitter OAuth")
		
		configs.foreach{ case(key, value) => 
			if (value.trim.isEmpty)
				throw new Exception("Error setting authentication - value for " + key + " not set -> value is " + value)
			val fullKey = "twitter4j.oauth." + key.replace("api", "consumer")
			System.setProperty(fullKey, value.trim)
			println("\tProperty " + fullKey + " set as [" + value.trim + "]") }
		
		println()
	}
}
