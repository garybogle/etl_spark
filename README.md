# etl_spark

This project can be built with this command:

`sbt package assembly`

The project can be run with the current jar with this command:

`spark-submit --class "org.garybogle.SayariAssignment" --master "local[*]" target/scala-2.12/sayari-assignment-sbt-assembly-fatjar-1.0.jar sdn.csv add.csv alt.csv conList.csv`
