name := "NEDataConsumption"

version := "1.0"

scalaVersion := "2.11.12"

val sparkVersion = "2.3.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "com.databricks" %% "spark-avro" % "4.0.0",
  "org.apache.avro" % "avro" % "1.8.2"
)

mainClass in assembly := some("com.tdp.genesis.spark.DataConsumption")
assemblyJarName := "TdpNovumNEDataConsumptionApp_2.0.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}