package com.coxautodata.waimak.dataflow.spark.dataquality.deequ.prefabchecks

import java.sql.Timestamp
import java.time.LocalDateTime

import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.{VerificationRunBuilder, VerificationRunBuilderWithRepository}
import com.coxautodata.waimak.dataflow.spark.dataquality.deequ.{DeequCheckException, DeequPrefabCheck}

/**
  * Checks that the most recent value in a timestamp column is within a configured number of hours to now (default is 6 hours).
  * The purpose of this is to flag up when our data is unexpectedly stale.
  */
class RecentTimestampCheck extends DeequPrefabCheck[RecentTimestampCheckConfig] {
  override protected def checks(conf: RecentTimestampCheckConfig): Option[VerificationRunBuilder => VerificationRunBuilder] = {
    Some(_.addCheck(Check(conf.checkLevel, s"${conf.alertLevel}_checks")
      .hasSize(_ > 0, Some(s"No new data in the last ${conf.hoursToLookBack} hours."))
      .where(s"${conf.col} >= '${conf.checkTimestamp}'")))
  }

  override protected def anomalyChecks(conf: RecentTimestampCheckConfig): Option[VerificationRunBuilderWithRepository => VerificationRunBuilderWithRepository] = None

  override def checkName: String = "recentTimestampCheck"
}

case class RecentTimestampCheckConfig(col: String
                                      , hoursToLookBack: Int = 6
                                      , alertLevel: String = "warning"
                                      , nowOverride: Option[String] = None) {
  val checkLevel: CheckLevel.Value = alertLevel match {
    case "warning" => CheckLevel.Warning
    case "critical" => CheckLevel.Error
    case _ => throw DeequCheckException(s"Invalid alert level $alertLevel for recentTimestampCheck on column $col")
  }

  val checkTimestamp: Timestamp = Timestamp.valueOf(
    nowOverride.map(Timestamp.valueOf(_).toLocalDateTime).getOrElse(LocalDateTime.now()).minusHours(hoursToLookBack)
  )
}


