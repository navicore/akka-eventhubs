name := "AkkaEventhubs"
organization := "navicore"

fork := true
javaOptions in test ++= Seq(
  "-Xms512M", "-Xmx2048M",
  "-XX:MaxPermSize=2048M",
  "-XX:+CMSClassUnloadingEnabled"
)

parallelExecution in test := false

version := "0.1.0"

//scalaVersion := "2.12.4"
crossScalaVersions := Seq("2.11.12", "2.12.4")

ensimeScalaVersion in ThisBuild := "2.12.4"
val akkaVersion = "2.5.6"

publishArtifact in packageDoc := false
publishArtifact in packageSrc := false
//publishTo := Some(Resolver.file("file", new File("/Users/navicore/tmp/nexus")))
//publishTo := Some("Sonatype Snapshots Nexus" at "https://oss.sonatype.org/content/repositories/snapshots")

// POM settings for Sonatype
homepage := Some(url("https://github.com/navicore/akka-eventhubs"))

scmInfo := Some(ScmInfo(url("https://github.com/navicore/akka-eventhubs"),
                            "git@github.com:navicore/akka-eventhubs.git"))

developers := List(Developer("navicore",
                             "Ed Sweeney",
                             "ed@onextent.com",
                             url("https://github.com/navicore")))
licenses += ("MIT", url("https://opensource.org/licenses/MIT"))
publishMavenStyle := true

// Add sonatype repository settings
publishTo := Some(
  if (isSnapshot.value)
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
)

libraryDependencies ++=
  Seq(
    "com.microsoft.azure" % "azure-eventhubs" % "0.15.1",

    "ch.qos.logback" % "logback-classic" % "1.1.7",
    "com.typesafe" % "config" % "1.2.1",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",

    "com.typesafe.akka" %% "akka-actor" % akkaVersion,
    "com.typesafe.akka" %% "akka-stream" % akkaVersion,

    "org.scalatest" %% "scalatest" % "3.0.1" % "test"
  )

dependencyOverrides ++= Seq(
  "com.typesafe.akka" %% "akka-actor"  % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion
)

mainClass in assembly := Some("onextent.akka.eventhubs.Main")
assemblyJarName in assembly := "AkkaEventhubs.jar"

assemblyMergeStrategy in assembly := {
  case PathList("reference.conf") => MergeStrategy.concat
  case x if x.endsWith("io.netty.versions.properties") => MergeStrategy.first
  case PathList("META-INF", _ @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

