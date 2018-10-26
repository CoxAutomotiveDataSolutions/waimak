package com.coxautodata.waimak.dataflow

class SimpleDataFlow[C](val inputs: DataFlowEntities
                           , val actions: Seq[DataFlowAction[C]]
                           , val tagState: DataFlowTagState
                           , override val flowContext: C
                           , val schedulingMeta: SchedulingMeta[C]
                           , val commitMeta: CommitMeta[C, DataFlow[C]]) extends DataFlow[C] {


//  override def commitMeta: CommitMeta[SimpleDataFlow.this.type] = commitM.asInstanceOf[CommitMeta[SimpleDataFlow.this.type]]

  override protected def createInstance(in: DataFlowEntities, ac: Seq[DataFlowAction[C]], tags: DataFlowTagState, schMeta: SchedulingMeta[C], comMeta: CommitMeta[C, DataFlow[C]]): SimpleDataFlow[C] =
    new SimpleDataFlow[C](in, ac, tags, flowContext, schMeta, comMeta)


}

class EmptyFlowContext

object SimpleDataFlow {

  def empty[C](flowContext: C) = new SimpleDataFlow[C](DataFlowEntities.empty, Seq.empty, DataFlowTagState(Set.empty, Set.empty, Map.empty), flowContext, new SchedulingMeta[C](), CommitMeta.empty[C])

  def empty() = new SimpleDataFlow[EmptyFlowContext](DataFlowEntities.empty, Seq.empty, DataFlowTagState(Set.empty, Set.empty, Map.empty), new EmptyFlowContext, new SchedulingMeta[EmptyFlowContext](), CommitMeta.empty[EmptyFlowContext])

  def noPool[C](poolName: String, context: EmptyFlowContext): Unit = ()

}
