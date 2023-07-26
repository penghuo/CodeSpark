name := "sql-job"

version := "1.0"

scalaVersion := "2.12.15"

val sparkVersion = "3.3.1"

assemblyJarName in assembly := "sql-job.jar"

mainClass in assembly := Some("org.opensearch.sql.SQLJob")

resolvers ++= Seq(
  ("apache-snapshots" at "http://repository.apache.org/snapshots/").withAllowInsecureProtocol(true)
)

scalacOptions ++= Seq("-target:jvm-1.8", "-Ywarn-unused:imports")
javacOptions ++= Seq("-source", "1.8")
Compile / compile / javacOptions ++= Seq("-target", "1.8")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
)
