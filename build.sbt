name := "sql-job"

version := "1.0"

scalaVersion := "2.12.15"

val sparkVersion = "3.3.2"

assemblyJarName in assembly := "sql-job.jar"

mainClass in assembly := Some("org.opensearch.sql.SQLJob")

resolvers ++= Seq(
  ("apache-snapshots" at "http://repository.apache.org/snapshots/").withAllowInsecureProtocol(true)
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
)
