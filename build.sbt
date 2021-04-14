import sbt._
import Dependencies._

lazy val scala212 = "2.12.13"
lazy val scala211 = "2.11.12"

// Configs for spark
val s24 = "2.4.5"
val s30 = "3.0.2"
val s31 = "3.1.1"

lazy val defaultSparkVersion = s30
lazy val defaultScalaVersion = scala212

//ThisBuild / crossScalaVersions := Seq(scala212, scala211)

lazy val sparkVers = sys.env.getOrElse("SPARK_VERSION", defaultSparkVersion)
lazy val scalaVers = sys.env.getOrElse("SCALA_VERSION", defaultScalaVersion)

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
    "org.apache.spark" %% "spark-core" % sparkVers % Provided,
    "org.apache.spark" %% "spark-sql" % sparkVers % Provided,
    "org.apache.spark" %% "spark-hive" % sparkVers % Provided,
    "org.apache.commons" % "commons-lang3" % "3.9",
    "org.scalatest" %% "scalatest" % "3.0.8" % Test
  ),
  Test / parallelExecution := false,
  Global / concurrentRestrictions += Tags.limit(Tags.Test, 1)
)

lazy val root = (project in file("."))
  .aggregate(core, app, databricksConf)

lazy val core = (project in file("waimak-core"))
  .settings(
    libraryDependencies ++= Seq(
      "com.beachape" %% "enumeratum" % "1.6.1"
    ))
  .settings(common: _*)

lazy val app = (project in file("waimak-app"))
  .settings(common: _*)
  .dependsOn(core)

lazy val databricksConf = (project in file("waimak-configuration-databricks"))
  .settings(common: _*)
  .settings(
    libraryDependencies ++= Seq(
      "com.databricks" %% "dbutils-api" % "0.0.5"
    )
  ).dependsOn(core)

lazy val storage = (project in file("waimak-storage"))
  .settings(common: _*)
  .dependsOn(core % "compile->compile;test->test;provided->provided")

lazy val dataquality = (project in file("waimak-dataquality"))
  .settings(common: _*)
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.commons" % "commons-email" % "1.5",
      "io.circe" %% "circe-core" % "0.11.1",
      "io.circe" %% "circe-generic" % "0.11.1",
      "org.jvnet.mock-javamail" % "mock-javamail" % "1.9"
    )
  ).dependsOn(core, storage)

// Disabled for now due to https://github.com/awslabs/deequ/issues/353 and
// https://github.com/awslabs/deequ/issues/354

//lazy val deequDep = getDeequDependency(scalaVers, sparkVers)
//lazy val deequ = (project in file("waimak-deequ"))
//  .settings(common: _*)
//  .settings(
//    scalaVersion := scalaVers,
//    libraryDependencies ++= Seq(
//      deequDep
//    )
//  ).dependsOn(core, storage, dataquality)

lazy val experimental = (project in file("waimak-experimental"))
  .settings(common: _*)
  .settings(
    scalaVersion := scalaVers,
    libraryDependencies ++= Seq()
  ).dependsOn(core)

lazy val hive = (project in file("waimak-hive"))
  .settings(common: _*)
  .settings(
    scalaVersion := scalaVers,
    libraryDependencies ++= Seq()
  ).dependsOn(core)

lazy val impala = (project in file("waimak-impala"))
  .settings(common: _*)
  .settings(
    scalaVersion := scalaVers,
    libraryDependencies ++= Seq()
  ).dependsOn(core)

val testcontainersScalaVersion = "0.39.3"

lazy val rdbm = (project in file("waimak-rdbm-ingestion"))
  .settings(common: _*)
  .settings(
    scalaVersion := scalaVers,
    libraryDependencies ++= Seq(
      "org.postgresql" % "postgresql" % "42.2.2" % Provided,
      "com.microsoft.sqlserver" % "mssql-jdbc" % "8.4.1.jre8" % Provided,
      "com.dimafeng" %% "testcontainers-scala-scalatest" % testcontainersScalaVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-postgresql" % testcontainersScalaVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-mssqlserver" % testcontainersScalaVersion % Test,
    )
  ).dependsOn(core % "compile->compile;test->test;provided->provided", storage)