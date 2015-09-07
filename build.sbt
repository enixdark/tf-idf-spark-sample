name := "sp"

version := "1.0"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.4.1",
  "org.apache.spark" %% "spark-mllib" % "1.4.1",
  "com.databricks" % "spark-csv_2.11" % "1.2.0",
  "net.sf.opencsv" % "opencsv" % "2.3"
)
    
