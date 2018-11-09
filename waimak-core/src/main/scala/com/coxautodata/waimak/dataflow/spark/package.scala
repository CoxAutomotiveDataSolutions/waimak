package com.coxautodata.waimak.dataflow

package object spark {

  type CleanUpStrategy[T] = (String, Seq[T]) => Seq[T]

}
