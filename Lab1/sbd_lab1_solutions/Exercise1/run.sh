$SPARK_HOME/bin/spark-submit --class "Pagecounts" --master local[*] --driver-memory 4g target/scala-2.10/Pagecounts-assembly-0.1-SNAPSHOT.jar $1
