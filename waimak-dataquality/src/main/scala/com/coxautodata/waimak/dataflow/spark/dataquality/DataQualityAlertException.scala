package com.coxautodata.waimak.dataflow.spark.dataquality

class DataQualityAlertException(val text: String, val cause: Throwable = null) extends RuntimeException(text, cause)