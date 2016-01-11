name := "spark-scala-demo"

version := "1.0"
scalaVersion := "2.10.6"
sbtVersion := "0.13.9"
libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.5.2"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.5.2"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.5.2"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.1" excludeAll ExclusionRule(organization = "javax.servlet")

