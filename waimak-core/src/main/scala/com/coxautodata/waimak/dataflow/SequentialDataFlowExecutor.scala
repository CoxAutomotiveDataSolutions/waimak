package com.coxautodata.waimak.dataflow

import com.coxautodata.waimak.log.Logging

import scala.annotation.tailrec

/**
  * Created by Alexei Perelighin 2017/12/27
  *
  * Executes one action at a time wihtout trying to parallelize them.
  *
  * @tparam T the type of the entity which we are transforming (e.g. Dataset
  * @tparam C the type of context which we pass to the actions
  */
class SequentialDataFlowExecutor[T, C](override val flowReporter: FlowReporter[T, C]) extends DataFlowExecutor[T, C] with Logging {

  //TODO: Not sure that this executor will stay the same after proper parallelization. But the flow methods will definitely stay the same
  /**
    * Schedules and executes all of the inputs of the data flow and returns final DataFlow state when it can no longer
    * execute any actions.
    *
    * It does feel like it should not be doing waves, but the methods used in it are developed for multithreaded executor, this one is just temporary.
    *
    * @param dataFlow - input data flow
    * @return - final state after a wave is executed
    */
  def executeWave(dataFlow: DataFlow[T, C]): (Seq[DataFlowAction[T, C]], DataFlow[T, C]) = {
    val wave = dataFlow.nextRunnable()
    logInfo(s"Scheduling wave of ${wave.size} actions:")
    wave.foreach { action => logInfo(action.logLabel) }
    val resFlow = wave.foldLeft(dataFlow) { (df, action) =>
      val inputEntities: DataFlowEntities[T] = {
        if (action.requiresAllInputs) df.inputs.filterLabels(action.inputLabels).map(_.get)
        else df.inputs.filterLabels(action.inputLabels).filterValues(_.isDefined).map(_.get)
      }

      logInfo(s"Submitting action ${action.logLabel}")
      //TODO: left for compatibility, need to change the data flow entities to know about optional
      val actionOutputs: Seq[Option[T]] = executeAction(action, inputEntities, dataFlow.flowContext)
      df.executed(action, actionOutputs)
    }
    (wave, resFlow)
  }

  /**
    * Executes as many actions as possible with the given DAG
    *
    * @param dataFlow
    * @return (Seq[EXECUTED ACTIONS], FINAL STATE). Final state does not contain the executed actions and the outputs
    *         of the executed actions are now in the inputs
    */
  def execute(dataFlow: DataFlow[T, C]): (Seq[DataFlowAction[T, C]], DataFlow[T, C]) = {

    @tailrec
    def loop(allExecutedActions: Seq[DataFlowAction[T, C]], flow: DataFlow[T, C]): (Seq[DataFlowAction[T, C]], DataFlow[T, C]) = executeWave(flow) match {
      case (nothingWasExecuted, finalFlow) if nothingWasExecuted.isEmpty => (allExecutedActions, finalFlow)
      case (executedInWave, intermediateFlow) => loop(allExecutedActions ++ executedInWave, intermediateFlow)
    }

    val preparedDataFlow = dataFlow.prepareForExecution()

    loop(Seq.empty, preparedDataFlow)
  }
}

