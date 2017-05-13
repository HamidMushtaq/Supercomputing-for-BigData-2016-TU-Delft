$SPARK_HOME/bin/spark-submit --class "FastqChunker" --master local[*] --driver-memory 2g ./target/scala-2.11/fastqchunker_2.11-1.0.jar $1 $2 $3
