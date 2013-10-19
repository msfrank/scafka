import sbt._
import Keys._

object ScafkaBuild extends Build {

  val scafkaVersion = "0.1-SNAPSHOT"
  val akkaVersion = "2.2.1"
  val curatorVersion = "1.3.3"
  val jacksonVersion = "2.2.3"

  lazy val scafkaBuild = Project(
    id = "scafka",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      exportJars := true,
      name := "scafka",
      version := scafkaVersion,
      scalaVersion := "2.10.2",
      scalacOptions ++= Seq("-feature", "-deprecation"),
      javacOptions ++= Seq("-source", "1.7"),
      libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-actor" % akkaVersion,
        "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
        "com.netflix.curator" % "curator-recipes" % curatorVersion,
        "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
        "nl.grons" %% "metrics-scala" % "3.0.3" excludeAll ExclusionRule(organization = "com.typesafe.akka"),
        "org.slf4j" % "slf4j-api" % "1.7.5",
        "org.scalatest" %% "scalatest" % "1.9.1" % "test",
        "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
        "org.slf4j" % "slf4j-log4j" % "1.7.5" % "test",
        "log4j" % "log4j" % "1.2.16" % "test"
      )
    )
  ).settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
}
