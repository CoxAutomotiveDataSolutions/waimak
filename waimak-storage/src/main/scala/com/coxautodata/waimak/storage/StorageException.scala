package com.coxautodata.waimak.storage

/**
  * Is thrown by storage layer.
  *
  * Created by Alexei Perelighin on 2018/03/04
  */
case class StorageException(text: String, cause: Throwable = null) extends RuntimeException(text, cause)
