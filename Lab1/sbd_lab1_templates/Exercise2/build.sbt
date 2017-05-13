import AssemblyKeys._ // put this at the top of the file

name := "Twitterstats"

scalaVersion := "2.11.0"

libraryDependencies ++= Seq(
	"org.apache.spark" % "spark-streaming_2.11" % "1.5.2",
	"org.apache.spark" % "spark-streaming-twitter_2.11" % "1.5.2",
	"org.apache.tika" % "tika-core" % "1.13",
	"org.apache.tika" % "tika-parsers" % "1.13"
)

resourceDirectory in Compile := baseDirectory.value / "resources"

assemblySettings

mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}

