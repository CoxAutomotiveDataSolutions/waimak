package com.coxautodata.waimak.dataflow.spark

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset}

import com.coxautodata.waimak.dataflow.spark.SparkActions._
import com.coxautodata.waimak.dataflow.{ActionResult, DataFlowEntities}
import com.coxautodata.waimak.storage.AuditTableInfo
import com.coxautodata.waimak.storage.StorageActions._
import io.circe
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.{PostMethod, StringRequestEntity}
import org.apache.http.client.HttpResponseException
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Dataset, Encoder}

import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

object DataQualityActions {

  implicit class DataQualityActionImplicits(sparkDataFlow: SparkDataFlow) {

    def monitor(labels: String*)(rules: DataQualityRule[_]*)(alerts: DataQualityAlertHandler*)(storage: DataQualityMetricStorage): SparkDataFlow = {
      sparkDataFlow
        .asInstanceOf[SparkDataFlow]
        .foldLeftOver(labels) {
          (z, l) => z.asInstanceOf[SparkDataFlow].foldLeftOver(rules)((zz, r) => zz.addRule(l, r, storage, alerts))
        }
    }

    private[spark] def addRule(label: String, rule: DataQualityRule[_], storage: DataQualityMetricStorage, alerts: Seq[DataQualityAlertHandler]): SparkDataFlow = {
      sparkDataFlow
        .cacheAsParquet(label)
        .transform(label)(rule.produceMetricLabel(label))(rule.produceMetric)
        .tag(rule.writeMetricTag(label)) {
          _.subFlow(storage.addMetricWriteToFlow(label, rule))
        }
        .tagDependency(rule.writeMetricTag(label)) {
          _.subFlow(storage.addMetricReadToFlow(label, rule))
        }
        .transform(rule.toReduceMetricLabel(label))(rule.reducedMetricLabel(label))(rule.reduceMetricsUntyped)
        .addAction(AlertAction(label, rule, alerts))
    }

    private[spark] def subFlow(sub: SparkDataFlow => SparkDataFlow): SparkDataFlow = {
      sub(sparkDataFlow)
    }

  }

}

case class AlertAction(label: String, rule: DataQualityRule[_], alerts: Seq[DataQualityAlertHandler]) extends SparkDataFlowAction {

  override def performAction(inputs: DataFlowEntities, flowContext: SparkFlowContext): Try[ActionResult] = Try {
    val reduced = inputs.get[Dataset[_]](rule.reducedMetricLabel(label))
    rule
      .thresholdTriggerUntyped(reduced, label)
      .foreach(a => alerts.foreach(_.handleAlert(a)))
    Seq.empty
  }

  override val inputLabels: List[String] = List(rule.reducedMetricLabel(label))
  override val outputLabels: List[String] = List.empty
}

abstract class DataQualityRule[T: TypeTag : Encoder] {

  //implicit val encoder: Encoder[T] = implicitly[Encoder[T]]

  final def baseMetricLabel(inputLabel: String): String = s"${inputLabel}_$name"

  final def produceMetricLabel(inputLabel: String): String = s"${baseMetricLabel(inputLabel)}_produced"

  final def toReduceMetricLabel(inputLabel: String): String = s"${baseMetricLabel(inputLabel)}_to_reduce"

  final def reducedMetricLabel(inputLabel: String): String = s"${baseMetricLabel(inputLabel)}_reduced"

  final def writeMetricTag(inputLabel: String): String = s"${baseMetricLabel(inputLabel)}_write"

  def name: String

  def produceMetric(ds: Dataset[_]): Dataset[T]

  def reduceMetrics(ds: Dataset[MetricRecord[T]]): Dataset[T]

  final def reduceMetricsUntyped(ds: Dataset[_]): Dataset[T] = {
    import ds.sparkSession.implicits._
    ds.as[MetricRecord[T]].transform(reduceMetrics)
  }

  def thresholdTrigger(ds: Dataset[T], label: String): Option[DataQualityAlert]

  final def thresholdTriggerUntyped(ds: Dataset[_], label: String): Option[DataQualityAlert] = {
    thresholdTrigger(ds.as[T], label: String)
  }

}

case class NullValuesRule(colName: String, percentageNullWarningThreshold: Int, percentageNullCriticalThreshold: Int)(implicit intEncoder: Encoder[Int]) extends DataQualityRule[Int] {

  val name: String = "null_values_check"

  override def produceMetric(ds: Dataset[_]): Dataset[Int] = {
    import ds.sparkSession.implicits.StringToColumn
    ds.withColumn("nulls_count", sum(when($"$colName".isNull, 1).otherwise(0)).over(Window.partitionBy()))
      .withColumn("total_count", count("*").over(Window.partitionBy()))
      .withColumn("perc_nulls", (($"nulls_count" / $"total_count") * 100).cast(IntegerType))
      .select("perc_nulls")
      .as[Int]
  }

  override def reduceMetrics(ds: Dataset[MetricRecord[Int]]): Dataset[Int] = {
    import ds.sparkSession.implicits.StringToColumn
    ds.withColumn("_row_num", row_number() over Window.partitionBy().orderBy($"dateTimeEmitted".desc))
      .filter($"_row_num" === 1)
      .select($"metric").as[Int]
  }

  override def thresholdTrigger(ds: Dataset[Int], label: String): Option[DataQualityAlert] = {
    ds.collect().headOption.filter(_ > percentageNullWarningThreshold.min(percentageNullCriticalThreshold)).map(perc => {
      val (alertImportance, thresholdUsed) = perc match {
        case p if p > percentageNullCriticalThreshold => (Critical, percentageNullCriticalThreshold)
        case _ => (Warning, percentageNullWarningThreshold)
      }
      DataQualityAlert(s"${alertImportance.description} alert for $name on label $label. Percentage of nulls in column $colName was $perc%. " +
        s"${alertImportance.description} threshold $thresholdUsed%", alertImportance)
    })
  }
}

case class UniqueIDsRule[T: Encoder: TypeTag](idColName: String, minUniqueIds: Long, minTimestamp: Timestamp) extends DataQualityRule[T] {
  override def name: String = "new_unique_ids_check"

  override def produceMetric(ds: Dataset[_]): Dataset[T] = {
    ds.select(idColName).distinct().as[T]
  }

  override def reduceMetrics(ds: Dataset[MetricRecord[T]]): Dataset[T] = {
    import ds.sparkSession.implicits.StringToColumn
    ds.filter($"dateTimeEmitted" >= minTimestamp)
      .select("metric")
      .distinct()
      .as[T]
  }

  override def thresholdTrigger(ds: Dataset[T], label: String): Option[DataQualityAlert] = {
    val newUniqueIDsCount = ds.count()
    Option(newUniqueIDsCount).filter(_ < minUniqueIds).map(idCount => DataQualityAlert(s"Alert for $name on label $label. " +
      s"Number of unique ids since $minTimestamp was $idCount. " +
      s"Expected at least $minUniqueIds", Critical))
  }
}


case class MetricRecord[T](dateTimeEmitted: Timestamp, metric: T)

trait DataQualityMetricStorage {

  def addMetricWriteToFlow(label: String, rule: DataQualityRule[_])(sparkDataFlow: SparkDataFlow): SparkDataFlow

  def addMetricReadToFlow(label: String, rule: DataQualityRule[_])(sparkDataFlow: SparkDataFlow): SparkDataFlow

}

case class DataQualityAlert(alertMessage: String, importance: AlertImportance)

case class SlackQualityAlert(token: String) extends DataQualityAlertHandler {

  private def toJson(alert: DataQualityAlert): String = {
    val slackColour = alert.importance match {
      case Critical => SlackDanger
      case Warning => SlackWarning
      case Good => SlackGood
      case Information => SlackInformation
    }
    SlackMessage(attachments = Some(Seq(SlackAttachment(Some(alert.alertMessage), color = Some(slackColour)))))
      .asJson
      .noSpaces
  }

  override def handleAlert(alert: DataQualityAlert): Unit = {
    val json = toJson(alert)
    val post = new PostMethod(s"https://hooks.slack.com/services/$token")
    post.setRequestHeader("Content-type", "application/json")
    post.setRequestEntity(new StringRequestEntity(json, "application/json", "UTF-8"))
    val response = new HttpClient().executeMethod(post)
    val responseStatus = response
    if (responseStatus != 200) {
      throw new HttpResponseException(responseStatus, s"Invalid response status, got $responseStatus")
    }
  }
}

sealed abstract class AlertImportance(val description: String)

case object Critical extends AlertImportance("Critical")

case object Warning extends AlertImportance("Warning")

case object Good extends AlertImportance("Good")

case object Information extends AlertImportance("Information")

trait DataQualityAlertHandler {
  def handleAlert(alert: DataQualityAlert): Unit
}

case class StorageLayerMetricStorage(basePath: String, runtime: LocalDateTime) extends DataQualityMetricStorage {
  override def addMetricWriteToFlow(label: String, rule: DataQualityRule[_])(sparkDataFlow: SparkDataFlow): SparkDataFlow = {
    sparkDataFlow
      .getOrCreateAuditTable(
        basePath,
        Some(t => AuditTableInfo(t, Seq("metric"), Map.empty, retain_history = true))
      )(rule.baseMetricLabel(label))
      .transform(rule.produceMetricLabel(label))(rule.baseMetricLabel(label)) {
        _.toDF("metric")
          .withColumn("dateTimeEmitted", lit(Timestamp.valueOf(runtime)))
      }
      .writeToStorage(rule.baseMetricLabel(label), "dateTimeEmitted", runtime.atZone(ZoneOffset.UTC), (_, _, _) => true)
  }

  override def addMetricReadToFlow(label: String, rule: DataQualityRule[_])(sparkDataFlow: SparkDataFlow): SparkDataFlow = {
    sparkDataFlow
      .loadFromStorage(basePath, outputPrefix = Some("load_from"))(rule.baseMetricLabel(label))
      .alias(s"load_from_${rule.baseMetricLabel(label)}", rule.toReduceMetricLabel(label))
  }
}

sealed abstract class SlackColor(val value: String)

case object SlackDanger extends SlackColor("danger")

case object SlackWarning extends SlackColor("warning")

case object SlackGood extends SlackColor("good")

case object SlackInformation extends SlackColor("#439FE0")

object SlackColor {
  implicit val encodeSlackColor: io.circe.Encoder[SlackColor] = new circe.Encoder[SlackColor] {
    override def apply(a: SlackColor): Json = a.value.asJson
  }
}

case class SlackField(title: String, value: String)

case class SlackAttachment(title: Option[String] = None, title_link: Option[String] = None, color: Option[SlackColor] = None,
                           ts: Option[String] = None, footer: Option[String] = None, fields: Option[Seq[SlackField]] = None)

case class SlackMessage(text: Option[String] = None, attachments: Option[Seq[SlackAttachment]] = None)