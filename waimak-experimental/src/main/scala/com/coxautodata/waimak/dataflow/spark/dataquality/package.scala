package com.coxautodata.waimak.dataflow.spark

package object dataquality {

  implicit class DataQualityActionImplicits(sparkDataFlow: SparkDataFlow) {

    def addDataQualityCheck[CheckType <: DataQualityCheck[CheckType]](label: String
                                                                      , check: CheckType
                                                                      , alertHandler: DataQualityAlertHandler): SparkDataFlow = {
      sparkDataFlow
        .updateMetadataExtension[DataQualityMetadataExtension[CheckType]](DataQualityMetadataExtensionIdentifier[CheckType]()
        , {
          m =>
            val existing = m.map(_.meta).getOrElse(Nil)
            val newMeta = DataQualityMeta(label, alertHandler, check)
            Some(DataQualityMetadataExtension(existing :+ newMeta))
        })
    }
  }
}
