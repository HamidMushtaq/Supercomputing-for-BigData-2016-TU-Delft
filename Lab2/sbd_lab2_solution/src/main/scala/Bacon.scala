/* Bacon.scala */
/* Author: Hamid Mushtaq */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import org.apache.spark.storage.StorageLevel._
import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

import scala.collection.mutable.ArrayBuffer
import java.io._
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.Calendar

object Bacon 
{
	final val KevinBacon = "Bacon, Kevin (I)"	// This is how Kevin Bacon's name appears in the actors.list file
	final val Infinite = 100
	final val Distance = 6
	final val compressRDDs = true
	val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("sparkLog.txt"), "UTF-8"))
	
	def removeAfterDate(s: String) : String =
	{
		return s.substring(0, s.indexOf(')') + 1)
	}
	
	def getActor2MoviesList (s: String, g: Boolean) : ((String, Boolean), Array[String]) =
	{
		val lines = s.split('\t')
		val data = lines.map(x => x.replace("\n", "")).filter(x => x != "")
		val actor = data(0)
		val movie_list1 = data.slice(1, data.size)
		val movie_list = movie_list1.filter(x => x(0) != '"').map(x => removeAfterDate(x)).filter{
			x => 
			try {
				val s = x.indexOf('(')
				val e = x.indexOf(')')
				val ss = x.slice(s+1, e)
				ss.slice(0,4).toInt >= 2011 // All movies in this decade (2011 and onwards, that is).
			} 
			catch {
				case e: Exception => false
			}
		}
		
		if (movie_list.size != 0)
			return ((actor, g), movie_list)
		else
			return null
	}
	
	def getActor2Movie (x: (Long, Array[String])) : Array[(Long, String)] =
	{
		val actor = x._1
		val movie_list = x._2
		val a2m = ArrayBuffer[(Long, String)]()
		
		for (movie <- movie_list) 
			a2m.append((actor, movie))
				
		return a2m.toArray
	}
	
	def updateDistance(x: (Long, (Int, Iterable[Long])), dist: Int): Array[(Long, Int)] =
	{
		//(actor, (distance, actorlist))
		val actor = x._1
		val distanceAndLOA = x._2
		val distance = distanceAndLOA._1
		val actorList = distanceAndLOA._2
		val ad = ArrayBuffer[(Long, Int)]()
	
		if (distance == dist)
		{
			ad.append((actor, distance))
			for(a <- actorList)
				ad.append((a, distance + 1))
		}
		else
			ad.append((actor, distance))
				
		return ad.toArray
	}
	
	def getTimeStamp() : String =
	{
		return "[" + new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime()) + "] "
	}
	
	def main(args: Array[String]) 
	{
		val cores = args(0)
		val inputFileM = args(1)
		val inputFileF = args(2)
		
		println(inputFileM)
		println(inputFileF)
		
		if (!(new File(inputFileM).exists))
		{
			println("The file for male actors: " + inputFileM + ", does not exist!")
			System.exit(1)
		}
		if (!(new File(inputFileF).exists))
		{
			println("The file for female actors: " + inputFileF + ", does not exist!")
			System.exit(1)
		}
		
		val conf = new SparkConf().setAppName("Kevin Bacon app")
		conf.setMaster("local[" + cores + "]")
		conf.set("spark.cores.max", cores)
		if (compressRDDs)
			conf.set("spark.rdd.compress", "true")
		val sc = new SparkContext(conf)
		
		sc.addSparkListener(new SparkListener() 
		{
			override def onApplicationStart(applicationStart: SparkListenerApplicationStart) 
			{
				bw.write(getTimeStamp() + " Spark ApplicationStart: " + applicationStart.appName + "\n");
				bw.flush
			}

			override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) 
			{
				bw.write(getTimeStamp() + " Spark ApplicationEnd: " + applicationEnd.time + "\n");
				bw.flush
			}

			override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) 
			{
				val map = stageCompleted.stageInfo.rddInfos
				map.foreach(row => {
					if (row.isCached)
					{
						bw.write(getTimeStamp() + row.name + ": memsize = " + (row.memSize / 1000000) + "MB, rdd diskSize " + 
							row.diskSize + ", numPartitions = " + row.numPartitions + "-" + row.numCachedPartitions + "\n");
					}
					else if (row.name.contains("rdd_"))
						bw.write(getTimeStamp() + row.name + " processed!\n");
					bw.flush
				})
			}
		});
		
		println("Number of cores: " + args(0))
		println("Input files: " + inputFileM + " and " + inputFileF)
		
		// Comment these two lines if you want to see more verbose messages from Spark
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		var t0 = System.currentTimeMillis
		
		// Main code here ////////////////////////////////////////////////////
		val c = new Configuration(sc.hadoopConfiguration)
		c.set("textinputformat.record.delimiter", "\n\n")
		val inputM = sc.newAPIHadoopFile(inputFileM, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], c).coalesce(cores.toInt / 2)
		val inputF = sc.newAPIHadoopFile(inputFileF, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], c).coalesce(cores.toInt / 2)
		// ((actor, gender), movielist)
		// Hamid: If an actor has only acted in TV series, don't include him!
		val actor2ListOfMoviesM = inputM.map(x => getActor2MoviesList(x._2.toString, true)).filter(x => x != null)
		val actor2ListOfMoviesF = inputF.map(x => getActor2MoviesList(x._2.toString, false)).filter(x => x != null)
		val actor2ListOfMovies1 = actor2ListOfMoviesM.union(actor2ListOfMoviesF).zipWithIndex.map{case(((actor, gender), movielist), i) =>
			if (gender == true) ((actor, movielist), i) else ((actor, movielist), -1*(i+1))} 
		// ((actor, movielist), i)
		actor2ListOfMovies1.setName("rdd_actor2ListOfMovies1")
		actor2ListOfMovies1.persist(if (compressRDDs) MEMORY_ONLY_SER else MEMORY_ONLY)
		val kevinBacon = actor2ListOfMovies1.filter(x => x._1._1 == KevinBacon).first()
		val KevinBaconID = kevinBacon._2
		// (actor_id, movielist)
		val actor2ListOfMovies = actor2ListOfMovies1.map{case((a, ml), i) => (i, ml)}
		actor2ListOfMovies.setName("rdd_actor2ListOfMovies")
		// (actor, movie)
		val actor2movie = actor2ListOfMovies.flatMap(x => getActor2Movie(x))
		actor2movie.setName("rdd_actor2movie")
		// (movie, actor)
		val movie2actor = actor2movie.map{case(a, m) => (m, a)}
		movie2actor.setName("rdd_movie2actor")
		//(movie, (actor, actor))
		val movie2ActorActor = movie2actor.join(movie2actor)
		movie2ActorActor.setName("rdd_movie2ActorActor")
		// (actor, actor)
		val actor2actor = movie2ActorActor.map{case(m, (a1, a2)) => (a1, a2)}.filter(x => x._1 != x._2)
		actor2actor.setName("rdd_actor2actor")
		actor2actor.persist(if (compressRDDs) MEMORY_ONLY_SER else MEMORY_ONLY)
		// (actor, actorlist)
		// Sorting
		val actor2listOfActors = actor2actor.groupByKey()
		actor2listOfActors.setName("rdd_actor2listOfActors")
		actor2listOfActors.persist(if (compressRDDs) MEMORY_ONLY_SER else MEMORY_ONLY)
		// (actor, distance)
		var actor2distance = actor2actor.map(x => (x._1, if (x._1 == KevinBaconID) 0 else Infinite))
		actor2actor.unpersist()
			
		val countListActors = ArrayBuffer[Long]()
		val countListActresses = ArrayBuffer[Long]()
		var dist = 0
		var actorsAtDist: org.apache.spark.rdd.RDD[(Long, Int)] = null
		for (dist <- 0 until Distance)
		{
			//(actor, (distance, actorlist))
			var A2distanceAndLOA = actor2distance.join(actor2listOfActors)
			actor2distance.unpersist()
			// (actor, distance)
			actor2distance = A2distanceAndLOA.flatMap(x => updateDistance(x, dist)).reduceByKey((x,y) => Math.min(x,y))
			actor2distance.setName("rdd_actor2distance")
			actor2distance.persist(if (compressRDDs) MEMORY_ONLY_SER else MEMORY_ONLY)
			actorsAtDist = actor2distance.filter(x => (x._2 == dist+1))
			countListActors.append(actorsAtDist.filter(x => x._1 >= 0).count)
			countListActresses.append(actorsAtDist.filter(x => x._1 < 0).count)
		}
			 
		val num_male_actors = actor2ListOfMovies1.filter(x => x._2 >= 0).count
		val num_female_actors = actor2ListOfMovies1.filter(x => x._2 < 0).count
		val num_of_actors = num_male_actors + num_female_actors
		val perc_male_actors = (num_male_actors * 100) / num_of_actors
		val perc_female_actors = (num_female_actors * 100) / num_of_actors
		val num_of_movies = movie2actor.groupByKey().count()
		// (actor_id, actor)
		val actorID2actor = actor2ListOfMovies1.map{case((actor, ml), id) => (id, actor)}
		// (actor_id, actor).join(actor_id, distance) -> (actor_id, (actor, distance))
		val x6 = actorID2actor.join(actorsAtDist).map{x => 
			val gender = if (x._1 >= 0) true else false
			(x._2._1, gender)
			}.sortByKey().collect()
		
		println( "Writing results to actors.txt" )
		val fout = new PrintWriter(new File("actors.txt" ))
		fout.write("Total number of actors = " + num_of_actors.toString() + ", out of which " + num_male_actors + 
			" (" + perc_male_actors + "%) are males while " + num_female_actors + " (" + perc_female_actors + "%) are females.\n")
		fout.write("Total number of movies = " + num_of_movies.toString() + "\n\n")
		var countActors: Long = 0
		var countActresses: Long = 0
		for (dist <- 0 until Distance)
		{
			countActors += countListActors(dist)
			countActresses += countListActresses(dist)
			val perc_actors = (countListActors(dist) * 100.0 / num_male_actors)
			val perc_actresses = (countListActresses(dist) * 100.0 / num_female_actors)
			
			fout.write("There are " + countListActors(dist) + " (" +  f"$perc_actors%.1f" + "%) actors and " + 
				countListActresses(dist) + " (" + f"$perc_actresses%.1f" + "%) actresses at distance " + (dist + 1) + "\n")
		}
		fout.write("\nTotal number of actors from distance 1 to 6 = " + (countActors + countActresses) + ", ratio = " + 
			((countActors + countActresses).toFloat / num_of_actors) + "\n")
		fout.write("Total number of male actors from distance 1 to 6 = " + countActors + ", ratio = " + 
			(countActors.toFloat / num_male_actors) + "\n" )
		fout.write("Total number of female actors (actresses) from distance 1 to 6 = " + countActresses + ", ratio = " + 
			(countActresses.toFloat / num_female_actors) + "\n") 
		fout.write( "\nList of male actors at distance 6:\n" )
		var index = 0
		for (x <- x6.filter(x => x._2 == true))
		{
			index += 1
			fout.write(index + ". " + x._1 + "\n") 
		}
		fout.write( "\nList of female actors (actresses) at distance 6:\n" )
		index = 0
		for (x <- x6.filter(x => x._2 == false))
		{
			index += 1
			fout.write(index + ". " + x._1 + "\n") 
		}
		fout.close()
		sc.stop()
		bw.close()
		
		val et = (System.currentTimeMillis - t0) / 1000
		val mins = et / 60
		val secs = et % 60
		println( "{Time taken = %d mins %d secs}".format(mins, secs) )
	} 
}
