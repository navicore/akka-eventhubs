name := "AkkaEventhubs"
fork := true
javaOptions in test ++= Seq(
  "-Xms512M",
  "-Xmx2048M",
  "-XX:MaxPermSize=2048M",
  "-XX:+CMSClassUnloadingEnabled"
)

parallelExecution in test := false

val akkaVersion = "2.6.20"

val scala212 = "2.12.17"
val scala213 = "2.13.10"

crossScalaVersions := Seq(scala212, scala213)
scalaVersion := scala212

ThisBuild / publishTo := sonatypePublishToBundle.value
ThisBuild / organization := "tech.navicore"
ThisBuild / homepage := Some(url("https://github.com/navicore/akka-eventhubs"))
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/navicore/akka-eventhubs"),
    "scm:git@github.com:navicore/akka-eventhubs.git"
  )
)
ThisBuild / licenses := List(
  "MIT" -> url(
    "https://github.com/navicore/akka-eventhubs/blob/master/LICENSE"
  )
)
ThisBuild / developers := List(
  Developer(
    "navicore",
    "Ed Sweeney",
    "ed@onextent.com",
    url("https://navicore.tech")
  )
)

libraryDependencies ++=
  Seq(
    "com.microsoft.azure" % "azure-eventhubs" % "3.3.0",
    "ch.qos.logback" % "logback-classic" % "1.4.4",
    "com.typesafe" % "config" % "1.4.2",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
    "com.typesafe.akka" %% "akka-actor" % akkaVersion,
    "com.typesafe.akka" %% "akka-stream" % akkaVersion,
    "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
    "org.scalatest" %% "scalatest" % "3.2.14" % "test"
  )

dependencyOverrides ++= Seq(
  "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion
)

assemblyJarName in assembly := "AkkaEventhubs.jar"

assemblyMergeStrategy in assembly := {
  case PathList("reference.conf")                      => MergeStrategy.concat
  case x if x.endsWith("io.netty.versions.properties") => MergeStrategy.first
  case PathList("META-INF", _ @_*)                     => MergeStrategy.discard
  case _                                               => MergeStrategy.first
}
