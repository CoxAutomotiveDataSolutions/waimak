import sbt._

object Dependencies {

  // Maybe needed in future for deequ
  def getDeequDependency(scalaVersion: String, sparkVersion: String) = {
    (scalaVersion.toMinVersion, sparkVersion.toMinVersion) match {
      case ("2.11", "2.4") => "com.amazon.deequ" % "deequ" % "1.2.2-spark-2.4"
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