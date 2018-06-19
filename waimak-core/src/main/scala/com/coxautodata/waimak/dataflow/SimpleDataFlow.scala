package com.coxautodata.waimak.dataflow

class SimpleDataFlow[T, C](val inputs: DataFlowEntities[Option[T]]
                           , val actions: Seq[DataFlowAction[T, C]]
                           , val tagState: DataFlowTagState
                           , override val flowContext: C) extends DataFlow[T, C] {

  override protected def createInstance(in: DataFlowEntities[Option[T]], ac: Seq[DataFlowAction[T, C]], tags: DataFlowTagState): SimpleDataFlow[T, C] =
    new SimpleDataFlow[T, C](in, ac, tags, flowContext)

}

class EmptyFlowContext

object SimpleDataFlow {

  def apply[T, C](inputs: DataFlowEntities[Option[T]]
                  , actions: Seq[DataFlowAction[T, C]]
                  , tags: DataFlowTagState
                  , flowContext: C) = new SimpleDataFlow[T, C](inputs, actions, tags, flowContext)

  def empty[T, C](flowContext: C) = new SimpleDataFlow[T, C](DataFlowEntities.empty, Seq.empty, DataFlowTagState(Set.empty, Set.empty, Map.empty), flowContext)

  def empty[T]() = new SimpleDataFlow[T, EmptyFlowContext](DataFlowEntities.empty, Seq.empty, DataFlowTagState(Set.empty, Set.empty, Map.empty), new EmptyFlowContext)

}
