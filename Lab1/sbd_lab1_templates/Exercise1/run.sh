$SPARK_HOME/bin/spark-submit --class "Pagecounts" --master local[*] target/scala-2.11/Pagecounts-assembly-0.1-SNAPSHOT.jar $1
