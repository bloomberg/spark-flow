name := "spark-flow"

organization := "com.bloomberg"

version := "0.1.0-SNAPSHOT"

val SPARK_VERSION = "2.0.0"
val JSON4S_VERSION = "3.3.0"

scalaVersion := "2.11.8"

resolvers ++= Seq("mvnrepository" at "http://mvnrepository.com/artifact/")

val jacksonExclusion = ExclusionRule(organization="com.fasterxml.jackson.core")
val asmExclusion = ExclusionRule(organization="asm")
val scalaExc = ExclusionRule(organization="org.scala-lang")
val hadoopExc = ExclusionRule(organization="org.apache.hadoop")

libraryDependencies ++= Seq(
  "org.ow2.asm" % "asm" % "5.1" withSources(),
  "org.apache.spark" %% "spark-core" % SPARK_VERSION % "provided",
  "org.apache.spark" %% "spark-sql" % SPARK_VERSION % "provided",
  "org.apache.spark" %% "spark-mllib" % SPARK_VERSION % "provided",
  "com.google.guava" % "guava" % "19.0",
  "com.holdenkarau" %% "spark-testing-base" % "2.0.0_0.4.4" % "test" excludeAll(asmExclusion, scalaExc, hadoopExc),
  "org.scalatest" %% "scalatest" % "2.2.1" % "test" withSources() excludeAll(scalaExc),
  "org.slf4j" % "slf4j-api" % "1.7.16",
  "org.slf4j" % "slf4j-log4j12" % "1.7.16"
)

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

parallelExecution in Test := false

credentials += Credentials(Path.userHome / ".sbt" / ".credentials")
