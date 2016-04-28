name := "SparkFlow"

organization := "bloomberg"

version := "0.0.1-SNAPSHOT"

val SPARK_VERSION = "1.6.0"
val JSON4S_VERSION = "3.3.0"

scalaVersion := "2.11.8"

resolvers ++= Seq("mvnrepository" at "http://mvnrepository.com/artifact/")

val jacksonExclusion = ExclusionRule(organization="com.fasterxml.jackson.core")
val asmExclusion = ExclusionRule(organization="asm")
val scalaExc = ExclusionRule(organization="org.scala-lang")
val hadoopExc = ExclusionRule(organization="org.apache.hadoop")

libraryDependencies ++= Seq(
  "org.ow2.asm" % "asm" % "5.1" withSources(),
  "org.apache.spark" %% "spark-core" % SPARK_VERSION excludeAll(asmExclusion, scalaExc),
  "org.apache.spark" %% "spark-sql" % SPARK_VERSION withSources() excludeAll(asmExclusion, scalaExc),
  "org.apache.spark" %% "spark-mllib" % SPARK_VERSION withSources() excludeAll(asmExclusion, scalaExc),
  "com.holdenkarau" %% "spark-testing-base" % "1.6.0_0.3.1" % "test" excludeAll(asmExclusion, scalaExc, hadoopExc),
  "org.scalatest" %% "scalatest" % "2.2.1" % "test" withSources() excludeAll(scalaExc),
  "com.databricks" % "spark-csv_2.11" % "1.4.0",
  "org.scala-lang" % "scala-compiler" % "2.11.8"
)

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

parallelExecution in Test := false
