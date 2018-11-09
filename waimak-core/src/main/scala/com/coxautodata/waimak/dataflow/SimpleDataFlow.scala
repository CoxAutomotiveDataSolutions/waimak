package com.coxautodata.waimak.dataflow

//class SimpleDataFlow1[C](val inputs: DataFlowEntities
//                           , val actions: Seq[DataFlowAction[C]]
//                           , val tagState: DataFlowTagState
//                           , override val flowContext: C
//                           , val schedulingMeta: SchedulingMeta[C]
//                           , val commitMeta: CommitMeta[C, SimpleDataFlow1[C]]) extends DataFlow[C, SimpleDataFlow1[C]] {
//
//
//
////  override protected def createInstance(in: DataFlowEntities, ac: Seq[DataFlowAction[C]], tags: DataFlowTagState, schMeta: SchedulingMeta[C], comMeta: CommitMeta[C, SimpleDataFlow[C]]): SimpleDataFlow[C] =
////    new SimpleDataFlow[C](in, ac, tags, flowContext, schMeta, comMeta)
//
//  /**
//    * All new states of the dataflow must be created via this factory method.
//    * This will allow specific dataflows to pass their specific context objects into new state.
//    *
//    * @param in - input entities for the next state
//    * @param ac - actions for the next state
//    * @return - new instance of the implementing class
//    */
//  override protected def createInstance(in: DataFlowEntities, ac: Seq[DataFlowAction[C]], tags: DataFlowTagState, schMeta: SchedulingMeta[C], comMeta: CommitMeta[C, SimpleDataFlow[C]]): DataFlow[C, SimpleDataFlow[C]] =
//    new SimpleDataFlow1(in, ac, tags, flowContext, schMeta, comMeta)
//}

class EmptyFlowContext extends FlowContext

//object SimpleDataFlow {
//
//  def empty[C](flowContext: C) = new SimpleDataFlow[C](DataFlowEntities.empty, Seq.empty, DataFlowTagState(Set.empty, Set.empty, Map.empty), flowContext, new SchedulingMeta[C](), CommitMeta())
//
//  def empty() = new SimpleDataFlow[EmptyFlowContext](DataFlowEntities.empty, Seq.empty, DataFlowTagState(Set.empty, Set.empty, Map.empty), new EmptyFlowContext, new SchedulingMeta[EmptyFlowContext](), CommitMeta())
//
//  def noPool[C](poolName: String, context: EmptyFlowContext): Unit = ()
//
//}
