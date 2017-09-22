del log.txt
%SPARK_HOME%\bin\spark-submit2.cmd --class "Twitterstats" target/scala-2.11/Twitterstats-assembly-0.1-SNAPSHOT.jar  >> log.txt 2>&1
pause
