package com.coxautodata.waimak.dataflow

/**
  * Created by Alexei Perelighin on 2018/11/08
  */
case class MockDataFlow(flowContext: FlowContext
                       , schedulingMeta: SchedulingMeta
                       , commitMeta: CommitMeta[MockDataFlow]
                       , inputs: DataFlowEntities
                       , actions: Seq[DataFlowAction]
                       , tagState: DataFlowTagState
                       , executor: DataFlowExecutor) extends DataFlow[MockDataFlow] {


  override def schedulingMeta(sc: SchedulingMeta): MockDataFlow = this.copy(schedulingMeta = sc)

  override def commitMeta(cm: CommitMeta[MockDataFlow]): MockDataFlow = this.copy(commitMeta = cm)

  override def inputs(inp: DataFlowEntities): MockDataFlow = this.copy(inputs = inp)

  override def actions(acs: Seq[DataFlowAction]): MockDataFlow = this.copy(actions = acs)

  override def tagState(ts: DataFlowTagState): MockDataFlow = this.copy(tagState = ts)

  override def withExecutor(executor: DataFlowExecutor): MockDataFlow = this.copy(executor = executor)
}

object MockDataFlow {

  def empty: MockDataFlow = MockDataFlow(new EmptyFlowContext, new SchedulingMeta(), CommitMeta[MockDataFlow](Map.empty, Map.empty), DataFlowEntities.empty, Seq.empty, DataFlowTagState(Set.empty, Set.empty, Map.empty), SequentialDataFlowExecutor(NoReportingFlowReporter.apply()))

}