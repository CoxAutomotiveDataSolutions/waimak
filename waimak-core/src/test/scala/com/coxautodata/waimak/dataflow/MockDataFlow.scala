package com.coxautodata.waimak.dataflow

/**
  * Created by Alexei Perelighin on 2018/11/08
  */
case class MockDataFlow(flowContext: FlowContext
                       , schedulingMeta: SchedulingMeta
                       , commitMeta: CommitMeta
                       , inputs: DataFlowEntities
                       , actions: Seq[DataFlowAction]
                       , tagState: DataFlowTagState) extends DataFlow {


  override def schedulingMeta(sc: SchedulingMeta): MockDataFlow.this.type = this.copy(schedulingMeta = sc).asInstanceOf[this.type]

  override def commitMeta(cm: CommitMeta): MockDataFlow.this.type = this.copy(commitMeta = cm).asInstanceOf[this.type]

  override def inputs(inp: DataFlowEntities): MockDataFlow.this.type = this.copy(inputs = inp).asInstanceOf[this.type]

  override def actions(acs: Seq[DataFlowAction]): MockDataFlow.this.type = this.copy(actions = acs).asInstanceOf[this.type]

  override def tagState(ts: DataFlowTagState): MockDataFlow.this.type = this.copy(tagState = ts).asInstanceOf[this.type]

}

object MockDataFlow {

  def empty: MockDataFlow = MockDataFlow(new EmptyFlowContext, new SchedulingMeta(), CommitMeta(Map.empty, Map.empty), DataFlowEntities.empty, Seq.empty, DataFlowTagState(Set.empty, Set.empty, Map.empty))

}