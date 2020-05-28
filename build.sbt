configs(IntegrationTest)
Defaults.itSettings
val TestAndIntegrationTest = "test,it"

organization in ThisBuild := "com.streetcontxt"
scalaVersion in ThisBuild := "2.13.1"
crossScalaVersions in ThisBuild := Seq("2.12.11", "2.13.1")
licenses in ThisBuild += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html"))
bintrayOrganization in ThisBuild := Some("streetcontxt")

resolvers in ThisBuild += Resolver.bintrayRepo("streetcontxt", "maven")

name := "kcl-akka-stream"

val versionPattern = "release-([0-9\\.]*)".r
version := sys.props
  .get("CIRCLE_TAG")
  .orElse(sys.env.get("CIRCLE_TAG"))
  .flatMap {
    case versionPattern(v) => Some(v)
    case _                 => None
  }
  .getOrElse("LOCAL-SNAPSHOT")

val AkkaVersion = "2.6.4"

val slf4j = "org.slf4j" % "slf4j-api" % "1.7.30"
val logback = "ch.qos.logback" % "logback-classic" % "1.2.3"
val amazonKinesisClient = "software.amazon.kinesis" % "amazon-kinesis-client" % "2.2.10"
val scalaKinesisProducer = "com.streetcontxt" %% "kpl-scala" % "1.1.0"
val scalaTest = "org.scalatest" %% "scalatest" % "3.1.2"
val scalaMock = "org.scalamock" %% "scalamock" % "4.4.0"
val akkaStream = "com.typesafe.akka" %% "akka-stream" % AkkaVersion
val akkaStreamTestkit = "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion

libraryDependencies ++= Seq(
  akkaStream,
  amazonKinesisClient,
  slf4j,
  scalaTest % TestAndIntegrationTest,
  akkaStreamTestkit % TestAndIntegrationTest,
  logback % TestAndIntegrationTest,
  scalaMock % Test,
  scalaKinesisProducer % IntegrationTest
)
