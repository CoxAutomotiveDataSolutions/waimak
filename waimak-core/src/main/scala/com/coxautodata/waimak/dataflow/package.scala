package com.coxautodata.waimak

/**
  * Created by Alexei Perelighin on 2018/01/11.
  */
package object dataflow {

  type ActionResult[T] = Seq[Option[T]]

}
