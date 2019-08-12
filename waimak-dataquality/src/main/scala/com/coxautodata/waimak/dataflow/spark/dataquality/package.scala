package com.coxautodata.waimak.dataflow.spark

package object dataquality {

  implicit class DataQualityActionImplicits(sparkDataFlow: SparkDataFlow) {

    /**
      * Add a data quality check for the given label
      *
      * @param label         the label to perform the check on
      * @param check         the data quality check to perform
      * @param alertHandler  the alert handler to use for handling alerts for this check
      * @param alertHandlers additional alert handlers to use
      * @tparam CheckType the type of the data quality check
      */
    def addDataQualityCheck[CheckType <: DataQualityCheck[CheckType]](label: String
                                                                      , check: CheckType
                                                                      , alertHandler: DataQualityAlertHandler
                                                                      , alertHandlers: DataQualityAlertHandler*): SparkDataFlow = {
      sparkDataFlow
        .updateMetadataExtension[DataQualityMetadataExtension[CheckType]](DataQualityMetadataExtensionIdentifier[CheckType]()
        , {
          m =>
            val existing = m.map(_.meta).getOrElse(Nil)
            val newMeta = DataQualityMeta(label, alertHandler +: alertHandlers, check)
            Some(DataQualityMetadataExtension(existing :+ newMeta))
        })
    }
  }

}
