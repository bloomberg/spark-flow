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
//  "com.holdenkarau" %% "spark-testing-base" % "1.6.0_0.3.1" % "test" excludeAll(asmExclusion, scalaExc, hadoopExc),
  "org.scalatest" %% "scalatest" % "2.2.1" % "test" withSources() excludeAll(scalaExc)
)

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

parallelExecution in Test := false

publishTo := {
  val artifactory = "http://artifactory.dev.bloomberg.com:8081/artifactory"
  if (isSnapshot.value) Some("bloomberg-artifactory-snapshots" at s"$artifactory/libs-snapshot-local")
  else Some("bloomberg-artifactory-releases" at s"$artifactory/libs-release-local")
}
credentials += Credentials(Path.userHome / ".sbt" / ".credentials")
