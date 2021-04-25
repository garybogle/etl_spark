name := "Sayari Assignment"

assembly / assemblyJarName := "sayari-assignment-sbt-assembly-fatjar-1.0.jar"
Compile / mainClass := Some("org.garybogle.SayariAssignment")
version := "1.0"

scalaVersion := "2.12.11"

libraryDependencies += "org.apache.spark" %% "spark-sql"  % "3.1.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.1.1" % "provided"
libraryDependencies += "com.github.vickumar1981" %% "stringdistance" % "1.2.4"
