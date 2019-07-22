package com.coxautodata.waimak.dataflow.spark.dataquality

case class DataQualityAlert(alertMessage: String, importance: AlertImportance)

sealed abstract class AlertImportance(val description: String)

case object Critical extends AlertImportance("Critical")

case object Warning extends AlertImportance("Warning")

case object Good extends AlertImportance("Good")

case object Information extends AlertImportance("Information")

trait DataQualityAlertHandler {
  def handleAlert(alert: DataQualityAlert): Unit
}
//
//case class AlertAction(label: String, rule: DataQualityRule[_], alerts: Seq[DataQualityAlertHandler]) extends SparkDataFlowAction {
//
//  override def performAction(inputs: DataFlowEntities, flowContext: SparkFlowContext): Try[ActionResult] = Try {
//    val reduced = inputs.get[Dataset[_]](rule.reducedMetricLabel(label))
//    rule
//      .thresholdTriggerUntyped(reduced, label)
//      .foreach(a => alerts.foreach(_.handleAlert(a)))
//    Seq.empty
//  }
//
//  override val inputLabels: List[String] = List(rule.reducedMetricLabel(label))
//  override val outputLabels: List[String] = List.empty
//}
//
//abstract class DataQualityRule[T: TypeTag : Encoder] {
//
//  final def baseMetricLabel(inputLabel: String): String = s"${inputLabel}_$name"
//
//  final def produceMetricLabel(inputLabel: String): String = s"${baseMetricLabel(inputLabel)}_produced"
//
//  final def toReduceMetricLabel(inputLabel: String): String = s"${baseMetricLabel(inputLabel)}_to_reduce"
//
//  final def reducedMetricLabel(inputLabel: String): String = s"${baseMetricLabel(inputLabel)}_reduced"
//
//  final def writeMetricTag(inputLabel: String): String = s"${baseMetricLabel(inputLabel)}_write"
//
//  def name: String
//
//  def produceMetric(ds: Dataset[_]): Dataset[T]
//
//  def reduceMetrics(ds: Dataset[MetricRecord[T]]): Dataset[T]
//
//  final def reduceMetricsUntyped(ds: Dataset[_]): Dataset[T] = {
//    import ds.sparkSession.implicits._
//    ds.as[MetricRecord[T]].transform(reduceMetrics)
//  }
//
//  def thresholdTrigger(ds: Dataset[T], label: String): Option[DataQualityAlert]
//
//  final def thresholdTriggerUntyped(ds: Dataset[_], label: String): Option[DataQualityAlert] = {
//    thresholdTrigger(ds.as[T], label: String)
//  }
//
//}
//
//case class NullValuesRule(colName: String, percentageNullWarningThreshold: Int, percentageNullCriticalThreshold: Int)(implicit intEncoder: Encoder[Int]) extends DataQualityRule[Int] {
//
//  val name: String = "null_values_check"
//
//  override def produceMetric(ds: Dataset[_]): Dataset[Int] = {
//    import ds.sparkSession.implicits.StringToColumn
//    ds.withColumn("nulls_count", sum(when($"$colName".isNull, 1).otherwise(0)).over(Window.partitionBy()))
//      .withColumn("total_count", count("*").over(Window.partitionBy()))
//      .withColumn("perc_nulls", (($"nulls_count" / $"total_count") * 100).cast(IntegerType))
//      .select("perc_nulls")
//      .as[Int]
//  }
//
//  override def reduceMetrics(ds: Dataset[MetricRecord[Int]]): Dataset[Int] = {
//    import ds.sparkSession.implicits.StringToColumn
//    ds.withColumn("_row_num", row_number() over Window.partitionBy().orderBy($"dateTimeEmitted".desc))
//      .filter($"_row_num" === 1)
//      .select($"metric").as[Int]
//  }
//
//  override def thresholdTrigger(ds: Dataset[Int], label: String): Option[DataQualityAlert] = {
//    ds.collect().headOption.filter(_ > percentageNullWarningThreshold.min(percentageNullCriticalThreshold)).map(perc => {
//      val (alertImportance, thresholdUsed) = perc match {
//        case p if p > percentageNullCriticalThreshold => (Critical, percentageNullCriticalThreshold)
//        case _ => (Warning, percentageNullWarningThreshold)
//      }
//      DataQualityAlert(s"${alertImportance.description} alert for $name on label $label. Percentage of nulls in column $colName was $perc%. " +
//        s"${alertImportance.description} threshold $thresholdUsed%", alertImportance)
//    })
//  }
//}
//
//case class UniqueIDsRule[T: Encoder : TypeTag](idColName: String, minUniqueIds: Long, minTimestamp: Timestamp) extends DataQualityRule[T] {
//  override def name: String = "new_unique_ids_check"
//
//  override def produceMetric(ds: Dataset[_]): Dataset[T] = {
//    ds.select(idColName).distinct().as[T]
//  }
//
//  override def reduceMetrics(ds: Dataset[MetricRecord[T]]): Dataset[T] = {
//    import ds.sparkSession.implicits.StringToColumn
//    ds.filter($"dateTimeEmitted" >= minTimestamp)
//      .select("metric")
//      .distinct()
//      .as[T]
//  }
//
//  override def thresholdTrigger(ds: Dataset[T], label: String): Option[DataQualityAlert] = {
//    val newUniqueIDsCount = ds.count()
//    Option(newUniqueIDsCount).filter(_ < minUniqueIds).map(idCount => DataQualityAlert(s"Alert for $name on label $label. " +
//      s"Number of unique ids since $minTimestamp was $idCount. " +
//      s"Expected at least $minUniqueIds", Critical))
//  }
//}
//
//
//case class MetricRecord[T](dateTimeEmitted: Timestamp, metric: T)
//
//trait DataQualityMetricStorage {
//
//  def addMetricWriteToFlow(label: String, rule: DataQualityRule[_])(sparkDataFlow: SparkDataFlow): SparkDataFlow
//
//  def addMetricReadToFlow(label: String, rule: DataQualityRule[_])(sparkDataFlow: SparkDataFlow): SparkDataFlow
//
//}

//
//case class StorageLayerMetricStorage(basePath: String, runtime: LocalDateTime) extends DataQualityMetricStorage {
//  override def addMetricWriteToFlow(label: String, rule: DataQualityRule[_])(sparkDataFlow: SparkDataFlow): SparkDataFlow = {
//    sparkDataFlow
//      .getOrCreateAuditTable(
//        basePath,
//        Some(t => AuditTableInfo(t, Seq("metric"), Map.empty, retain_history = true))
//      )(rule.baseMetricLabel(label))
//      .transform(rule.produceMetricLabel(label))(rule.baseMetricLabel(label)) {
//        _.toDF("metric")
//          .withColumn("dateTimeEmitted", lit(Timestamp.valueOf(runtime)))
//      }
//      .writeToStorage(rule.baseMetricLabel(label), "dateTimeEmitted", runtime.atZone(ZoneOffset.UTC), (_, _, _) => true)
//  }
//
//  override def addMetricReadToFlow(label: String, rule: DataQualityRule[_])(sparkDataFlow: SparkDataFlow): SparkDataFlow = {
//    sparkDataFlow
//      .loadFromStorage(basePath, outputPrefix = Some("load_from"))(rule.baseMetricLabel(label))
//      .alias(s"load_from_${rule.baseMetricLabel(label)}", rule.toReduceMetricLabel(label))
//  }
//}
