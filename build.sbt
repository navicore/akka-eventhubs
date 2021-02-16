name := "AkkaEventhubs"
organization := "tech.navicore"
javacOptions ++= Seq("-source", "1.8", "-target", "1.8") 
scalacOptions ++= Seq(
  "-target:jvm-1.8"
)
fork := true
javaOptions in test ++= Seq(
  "-Xms512M", "-Xmx2048M",
  "-XX:MaxPermSize=2048M",
  "-XX:+CMSClassUnloadingEnabled"
)

parallelExecution in test := false

val akkaVersion = "2.6.12"
val scala212 = "2.12.13"
val scala213 = "2.13.4"
crossScalaVersions := Seq(scala212, scala213)

inThisBuild(List(
  organization := "tech.navicore",
  homepage := Some(url("https://github.com/navicore/akka-eventhubs")),
  licenses := List("MIT" -> url("https://github.com/navicore/akka-eventhubs/blob/master/LICENSE")),
  developers := List(
    Developer(
      "navicore",
      "Ed Sweeney",
      "ed@onextent.com",
      url("https://navicore.tech")
    )
  )
))

libraryDependencies ++=
  Seq(
    "com.microsoft.azure" % "azure-eventhubs" % "3.2.2",
    "ch.qos.logback" % "logback-classic" % "1.2.3",
    "com.typesafe" % "config" % "1.4.1",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",

    "com.typesafe.akka" %% "akka-actor" % akkaVersion,
    "com.typesafe.akka" %% "akka-stream" % akkaVersion,
    "com.typesafe.akka" %% "akka-persistence" % akkaVersion,

    "org.scalatest" %% "scalatest" % "3.2.3" % "test"
  )

dependencyOverrides ++= Seq(
  "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
  "com.typesafe.akka" %% "akka-actor"  % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion
)

assemblyJarName in assembly := "AkkaEventhubs.jar"

assemblyMergeStrategy in assembly := {
  case PathList("reference.conf") => MergeStrategy.concat
  case x if x.endsWith("io.netty.versions.properties") => MergeStrategy.first
  case PathList("META-INF", _ @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

