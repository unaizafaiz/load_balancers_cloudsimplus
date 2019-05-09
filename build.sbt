name := "unaiza_faiz_project"

version := "0.1"

scalaVersion := "2.12.8"

// https://mvnrepository.com/artifact/org.cloudsimplus/cloudsim-plus
libraryDependencies ++= Seq(
  "org.cloudsimplus" % "cloudsim-plus" % "4.3.1",
  "junit" % "junit" % "4.12" % Test,
  "org.slf4j" % "slf4j-api" % "1.7.12",
  "com.typesafe" % "config" % "1.3.3",
  "org.scalatest" %% "scalatest" % "3.0.5",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "ch.qos.logback" % "logback-classic" % "1.2.3"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
