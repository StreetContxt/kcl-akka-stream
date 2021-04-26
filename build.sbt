inThisBuild(
  List(
    organization := "io.github.streetcontxt",
    homepage := Some(url("https://github.com/streetcontxt/kcl-akka-stream")),
    licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    developers := List(
      Developer(
        "agenovese",
        "Angelo Gerard Genovese",
        "angelo.gerard.genovese@gmail.com",
        url("https://github.com/agenovese")
      ),
      Developer(
        "elise-scx",
        "Elise Cormie",
        "elise@streetcontxt.com",
        url("https://github.com/elise-scx")
      )
    )
  )
)

configs(IntegrationTest)
Defaults.itSettings
val TestAndIntegrationTest = "test,it"

scalaVersion in ThisBuild := "2.13.3"
scalacOptions ++= Seq("-deprecation", "-feature")
crossScalaVersions in ThisBuild := Seq("2.12.11", "2.13.3")
licenses in ThisBuild += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html"))
sonatypeCredentialHost := "s01.oss.sonatype.org"

name := "kcl-akka-stream"

val AkkaVersion = "2.6.8"

val slf4j = "org.slf4j" % "slf4j-api" % "1.7.30"
val logback = "ch.qos.logback" % "logback-classic" % "1.2.3"
val amazonKinesisClient = "software.amazon.kinesis" % "amazon-kinesis-client" % "2.3.0"
val scalaKinesisProducer = "com.streetcontxt" %% "kpl-scala" % "1.1.0"
val scalaCollectionCompat = "org.scala-lang.modules" %% "scala-collection-compat" % "2.1.6"
val scalaTest = "org.scalatest" %% "scalatest" % "3.2.2"
val scalaMock = "org.scalamock" %% "scalamock" % "5.0.0"
val akkaStream = "com.typesafe.akka" %% "akka-stream" % AkkaVersion
val akkaStreamTestkit = "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion

libraryDependencies ++= Seq(
  akkaStream,
  amazonKinesisClient,
  slf4j,
  scalaCollectionCompat,
  scalaTest % TestAndIntegrationTest,
  akkaStreamTestkit % TestAndIntegrationTest,
  logback % TestAndIntegrationTest,
  scalaMock % Test,
  scalaKinesisProducer % IntegrationTest
)
