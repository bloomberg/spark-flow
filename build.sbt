name := "SparkFlow"

version := "0.0.1"

val SPARK_VERSION = "1.6.0"
val JSON4S_VERSION = "3.3.0"

scalaVersion := "2.10.5"

resolvers ++= Seq("mvnrepository" at "http://mvnrepository.com/artifact/")

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.1" % "test" withSources(),
  "org.apache.spark" %% "spark-core" % SPARK_VERSION withSources(),
  "org.apache.spark" %% "spark-sql" % SPARK_VERSION withSources(),
  "org.json4s" %% "json4s-jackson" % JSON4S_VERSION,
  "org.json4s" %% "json4s-ext" % JSON4S_VERSION,
  "com.holdenkarau" %% "spark-testing-base" % "1.6.0_0.3.1"
)

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

parallelExecution in Test := false
