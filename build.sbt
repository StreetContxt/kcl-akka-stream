configs(IntegrationTest)
Defaults.itSettings
val TestAndIntegrationTest = "test,it"

organization in ThisBuild := "com.streetcontxt"
scalaVersion in ThisBuild := "2.11.8"
crossScalaVersions in ThisBuild := Seq("2.11.8", "2.12.4")
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
    case _ => None
  }
  .getOrElse("LOCAL-SNAPSHOT")

val slf4j = "org.slf4j" % "slf4j-api" % "1.7.21"
val logback = "ch.qos.logback" % "logback-classic" % "1.2.3"
val amazonKinesisClient = "com.amazonaws" % "amazon-kinesis-client" % "1.8.8"
val scalaKinesisProducer = "com.streetcontxt" %% "kpl-scala" % "1.0.5"
val scalaTest = "org.scalatest" %% "scalatest" % "3.0.1"
val scalaMock = "org.scalamock" %% "scalamock-scalatest-support" % "3.6.0"
val akkaStream = "com.typesafe.akka" %% "akka-stream" % "2.5.11"
val akkaStreamTestkit = "com.typesafe.akka" %% "akka-stream-testkit" % "2.5.11"

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
