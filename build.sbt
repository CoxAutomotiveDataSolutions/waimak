import sbt._
import Dependencies._

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
  libraryDependencies ++= Dependencies.common,
  addCompilerPlugin("org.scalameta" % "semanticdb-scalac" % semanticDbVersion cross CrossVersion.full),
  scalacOptions += "-Yrangepos",
  Test / parallelExecution := false,
  Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-u", "target/test-reports", "-oID"),
  Global / concurrentRestrictions += Tags.limit(Tags.Test, 1),
  dependencyUpdatesFilter -= moduleFilter(organization = "org.apache.spark")
    | moduleFilter(organization = "org.scala-lang")
    | moduleFilter(organization = "org.apache.derby")
    | moduleFilter(organization = "com.microsoft.sqlserver")
)

lazy val root = (project in file("."))
  .settings(common,
    publishArtifact := false,
    Test / publishArtifact := false
  )
  .aggregate(core, app, databricksConf, storage, dataquality, experimental, hive, impala, rdbm)

lazy val core = (project in file("waimak-core"))
  .settings(common)
  .settings(
    libraryDependencies ++= Dependencies.core
  )

lazy val app = (project in file("waimak-app"))
  .settings(common)
  .dependsOn(core % "compile->compile;test->test;provided->provided")

lazy val databricksConf = (project in file("waimak-configuration-databricks"))
  .settings(common)
  .settings(
    libraryDependencies ++= Dependencies.databricks
  ).dependsOn(core % "compile->compile;test->test;provided->provided")

lazy val storage = (project in file("waimak-storage"))
  .settings(common)
  .dependsOn(core % "compile->compile;test->test;provided->provided")

lazy val dataquality = (project in file("waimak-dataquality"))
  .settings(common)
  .settings(
    libraryDependencies ++= Dependencies.dataquality
  ).dependsOn(core % "compile->compile;test->test;provided->provided", storage)

lazy val deequ = (project in file("waimak-deequ"))
  .settings(common)
  .settings(
    libraryDependencies ++= Dependencies.deequ
  ).dependsOn(core, storage, dataquality)

lazy val experimental = (project in file("waimak-experimental"))
  .settings(common)
  .dependsOn(core % "compile->compile;test->test;provided->provided")

lazy val hive = (project in file("waimak-hive"))
  .settings(common)
  .settings(
    libraryDependencies ++= Dependencies.hive
  ).dependsOn(core % "compile->compile;test->test;provided->provided")

lazy val impala = (project in file("waimak-impala"))
  .settings(common)
  .dependsOn(core % "compile->compile;test->test;provided->provided")

lazy val rdbm = (project in file("waimak-rdbm-ingestion"))
  .settings(common)
  .settings(
    libraryDependencies ++= Dependencies.rdbm
  ).dependsOn(core % "compile->compile;test->test;provided->provided", storage)