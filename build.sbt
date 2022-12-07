name := "CSC369_FinalProject"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.4.8"

resolvers ++= Seq(
  "apache-snapshots" at "https://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.scalanlp" %% "breeze" % "0.13.2",
  "org.scalanlp" %% "breeze-viz" % "0.13.2"
)