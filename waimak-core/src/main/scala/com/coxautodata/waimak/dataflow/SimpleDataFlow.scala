package com.coxautodata.waimak.dataflow

class SimpleDataFlow[C](val inputs: DataFlowEntities
                           , val actions: Seq[DataFlowAction[C]]
                           , val tagState: DataFlowTagState
                           , override val flowContext: C) extends DataFlow[C] {

  override protected def createInstance(in: DataFlowEntities, ac: Seq[DataFlowAction[C]], tags: DataFlowTagState): SimpleDataFlow[C] =
    new SimpleDataFlow[C](in, ac, tags, flowContext)

}

class EmptyFlowContext

object SimpleDataFlow {

  def apply[C](inputs: DataFlowEntities
                  , actions: Seq[DataFlowAction[ C]]
                  , tags: DataFlowTagState
                  , flowContext: C) = new SimpleDataFlow[C](inputs, actions, tags, flowContext)

  def empty[C](flowContext: C) = new SimpleDataFlow[C](DataFlowEntities.empty, Seq.empty, DataFlowTagState(Set.empty, Set.empty, Map.empty), flowContext)

  def empty() = new SimpleDataFlow[EmptyFlowContext](DataFlowEntities.empty, Seq.empty, DataFlowTagState(Set.empty, Set.empty, Map.empty), new EmptyFlowContext)

}
