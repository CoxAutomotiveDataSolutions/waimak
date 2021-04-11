import sbt._

lazy val scala212 = "2.12.13"
lazy val scala211 = "2.11.12"

// Configs for spark 2.4, 3.0 and 3.1
val s24 = "2.4.5"
val s30 = "3.0.2"
val s31 = "3.1.1"

lazy val defaultSparkVersion = s30
lazy val defaultScalaVersion = scala212

ThisBuild / crossScalaVersions := Seq(scala212, scala211)

lazy val sparkVers = sys.props.get("SPARK_VERSION").getOrElse(defaultSparkVersion)
lazy val scalaVers = sys.props.get("SCALA_VERSION").getOrElse(defaultScalaVersion)

ThisBuild / scalaVersion := scalaVers

val common = Def.settings(
  organization := "com.coxautodata",
  scalaVersion := scalaVers,
  developers := List(
    Developer(
      id = "alexi",
      name = "Alexei Perelighin",
      email = "alexeipab@gmail.com",
      url = url("http://coxautodata.com/")
    ),
    Developer(
      id = "vicky",
      name = "Vicky Avison",
      email = "vicky.avison@coxauto.co.uk",
      url = url("http://coxautodata.com/")
    ),
    Developer(
      id = "alex",
      name = "Alex Bush",
      email = "alex.bush@coxauto.co.uk",
      url = url("http://coxautodata.com/")
    ),
    Developer(
      id = "james",
      name = "James Fielder",
      email = "james@fielder.dev",
      url = url("https://james.fielder.dev/")
    )
  ),
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVers % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVers % "provided",
    "org.apache.spark" %% "spark-hive" % sparkVers % "provided",
    "org.apache.commons" % "commons-lang3" % "3.9",
    "org.scalatest" %% "scalatest" % "3.0.8" % "test"
  )
)

lazy val root = (project in file("."))
  .aggregate(core, app)

lazy val core = (project in file("waimak-core"))
  .settings(
    libraryDependencies ++= Seq(
      "com.beachape" %% "enumeratum" % "1.6.1"
    ))
  .settings(common: _*)

lazy val app = (project in file("waimak-app"))
  .settings(common: _*)
  .dependsOn(core)