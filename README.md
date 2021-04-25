# etl_spark

This project can be built with this command:

`sbt package assembly`
`spark-submit --class "org.garybogle.SayariAssignment" --master "local[*]" target/scala-2.12/sayari-assignment-sbt-assembly-fatjar-1.0.jar sdn.csv add.csv alt.csv conList.csv`

The project can be run with the current jar that is checked in with this command:

`spark-submit --class "org.garybogle.SayariAssignment" --master "local[*]" sayari-assignment-sbt-assembly-fatjar-1.0.jar sdn.csv add.csv alt.csv conList.csv`

My approach is to join the several SDN files into one data frame based on the SDN ent_num. This will produce a data frame with multiple rows for alternate names and multiple addresses. This is closer to the format that the ConList is in. 

The final schema includes data focused on Individuals and Companies/Organizations. I did not include the Vessel columns from the SDN list as ConList did not have a similar focus. 
