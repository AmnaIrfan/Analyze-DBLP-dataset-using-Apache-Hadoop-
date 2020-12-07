name := "amna_irfan_hw2"

version := "0.1"

scalaVersion := "2.13.1"


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.6.4",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe" % "config" % "1.3.4",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "org.scalatest" %% "scalatest" % "3.0.8" % "test",
  "org.apache.hadoop" % "hadoop-common" % "2.4.0" exclude("org.slf4j", "slf4j-log4j12"),
  "org.apache.hadoop" % "hadoop-client" % "2.4.0" exclude("org.slf4j", "slf4j-log4j12"),
  "org.scala-lang.modules" %% "scala-xml" % "1.2.0",
  "org.scala-lang.modules" % "scala-xml_2.13" % "1.2.0"
)

//Uncomment to run on Hadoop and EMR.
//mainClass in (Compile, run) := Some("org.airfan5.hw2.tasks.AuthorScoreMapReduce")
//mainClass in assembly := Some("org.airfan5.hw2.tasks.AuthorScoreMapReduce")

assemblyJarName in assembly := "amna_irfan_hw2.jar"
