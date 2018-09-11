package com.coxautodata.waimak.metastore

class MetastoreUtilsException(val text: String, val cause: Throwable = null) extends RuntimeException(text, cause)
