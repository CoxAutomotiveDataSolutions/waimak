package com.coxautodata.waimak.dataflow

package object spark {

  type CleanUpStrategy[T] = (Seq[T]) => Seq[T]

}
