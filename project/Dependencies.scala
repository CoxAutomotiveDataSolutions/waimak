import sbt._

object Dependencies {

  lazy val scala212 = "2.12.14"

  // Configs for spark
  val s30 = "3.0.2"
  val s31 = "3.1.2"

  lazy val defaultSparkVersion = s31
  lazy val defaultScalaVersion = scala212

  lazy val sparkVers = sys.env.getOrElse("SPARK_VERSION", defaultSparkVersion)
  lazy val scalaVers = sys.env.getOrElse("SCALA_VERSION", defaultScalaVersion)

  val commonsLang3Version = "3.12.0"
  val scalatestVersion = "3.2.9"
  val enumeratumVersion = "1.6.1"
  val dbutilsApiVersion = "0.0.5"
  val circeVersion = "0.14.1"
  val commonsEmailVersion = "1.5"
  val mockJavaMailVersion = "1.9"
  val testcontainersScalaVersion = "0.39.5"
  val postgresDriverVersion = "42.2.20"
  val mssqlDriverVersion = "9.2.1.jre8"
  val awaitilityVersion = "4.1.0"
  val semanticDbVersion = "4.4.21"

  // This has been updated to 10.14 in upstream spark, so watch out for that in 3.2 onwards
  val derbyVersion = "10.12.1.1"

  val common = Seq(
    // scala-steward:off
    "org.apache.spark" %% "spark-core" % sparkVers % Provided,
    "org.apache.spark" %% "spark-sql" % sparkVers % Provided,
    "org.apache.spark" %% "spark-hive" % sparkVers % Provided,
    // scala-steward:on
    "org.apache.commons" % "commons-lang3" % commonsLang3Version,
    "org.scalatest" %% "scalatest" % scalatestVersion % Test
  )

  val core = Seq(
    "com.beachape" %% "enumeratum" % enumeratumVersion
  )

  val databricks = Seq(
    "com.databricks" %% "dbutils-api" % dbutilsApiVersion
  )

  val dataquality = Seq(
    "org.apache.commons" % "commons-email" % commonsEmailVersion,
    "io.circe" %% "circe-core" % circeVersion,
    "io.circe" %% "circe-generic" % circeVersion,
    "org.jvnet.mock-javamail" % "mock-javamail" % mockJavaMailVersion % Test
  )

  lazy val deequDep = getDeequDependency(scalaVers, sparkVers)

  lazy val deequ = Seq(
    deequDep
  )

  val hive = Seq(
    // scala-steward:off
    "org.apache.derby" % "derbyclient" % derbyVersion % Test,
    "org.apache.derby" % "derbytools" % derbyVersion % Test,
    "org.apache.derby" % "derby" % derbyVersion % Test
    // scala-steward:on
  )

  val rdbm = Seq(
    "org.postgresql" % "postgresql" % postgresDriverVersion % Provided,
    "com.microsoft.sqlserver" % "mssql-jdbc" % mssqlDriverVersion % Provided, // scala-steward:off
    "com.dimafeng" %% "testcontainers-scala-scalatest" % testcontainersScalaVersion % Test,
    "com.dimafeng" %% "testcontainers-scala-postgresql" % testcontainersScalaVersion % Test,
    "com.dimafeng" %% "testcontainers-scala-mssqlserver" % testcontainersScalaVersion % Test,
    "org.awaitility" % "awaitility" % awaitilityVersion % Test
  )

  def getDeequDependency(scalaVersion: String, sparkVersion: String): ModuleID = {
    (scalaVersion.toMinVersion, sparkVersion.toMinVersion) match {
      case ("2.12", "3.0") | ("2.12", "3.1") => "com.amazon.deequ" % "deequ" % "1.2.2-spark-3.0"
      case _@(a, b) => throw new IllegalArgumentException(s"Deequ isn't available for Scala ${a} and Spark ${b}")
    }
  }

  implicit class VersionOps(version: String) {
    def toMinVersion: String = {
      version.split("\\.").toList match {
        case _ :: _ :: Nil => version
        case maj :: min :: _ :: Nil => s"$maj.$min"
        case _@v => throw new IllegalArgumentException(s"Cannot parse ${v.mkString("Array(", ", ", ")")} as version string")
      }
    }
  }
}