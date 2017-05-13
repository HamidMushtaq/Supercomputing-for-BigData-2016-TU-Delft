$SPARK_HOME/bin/spark-submit --class "AnalyzeTwitters" --master local[*] --driver-memory 4g target/scala-2.10/AnalyzeTwitters-assembly-0.1-SNAPSHOT.jar $1
