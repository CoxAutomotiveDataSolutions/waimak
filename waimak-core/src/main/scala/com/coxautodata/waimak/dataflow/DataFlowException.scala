package com.coxautodata.waimak.dataflow

class DataFlowException(val text: String, val cause: Throwable = null) extends RuntimeException(text, cause)
