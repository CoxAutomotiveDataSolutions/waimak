package com.coxautodata.waimak.dataflow

package object spark {

  type TableName = String
  type InputSnapshots[T] = Seq[T]
  type SnapshotsToDelete[T] = Seq[T]

  type CleanUpStrategy[T] = (TableName, InputSnapshots[T]) => SnapshotsToDelete[T]

}
