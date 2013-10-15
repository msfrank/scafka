import sbt._
import Keys._

object ScafkaBuild extends Build {

  val scafkaVersion = "0.1-SNAPSHOT"
  val akkaVersion = "2.2.1"
  val curatorVersion = "1.3.3"

  lazy val scafkaBuild = Project(
    id = "scafka",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      exportJars := true,
      name := "scafka",
      version := scafkaVersion,
      scalaVersion := "2.10.2",
      libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-actor" % akkaVersion,
        "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
        "com.netflix.curator" % "curator-recipes" % curatorVersion,
        "joda-time" % "joda-time" % "2.2",
        "nl.grons" %% "metrics-scala" % "3.0.3",
        "org.slf4j" % "slf4j-api" % "1.7.5",
        "org.scalatest" %% "scalatest" % "1.9.1" % "test",
        "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
        "ch.qos.logback" % "logback-classic" % "1.0.7" % "test"
      )
    )
  )
}
