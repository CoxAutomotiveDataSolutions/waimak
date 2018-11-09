package com.coxautodata.waimak.dataflow

/**
  * Defines various priority strategies for [[ DataFlowExecutor ]] to use.
  *
  * Created by Alexei Perelighin on 24/08/2018.
  */
object DFExecutorPriorityStrategies {

  type actionQueue = Seq[DataFlowAction]

  type priorityStrategy = actionQueue => actionQueue

  /**
    * Default strategy, at the moment it will not do anything.
    *
    * @tparam C
    * @return
    */
  def defaultPriorityStrategy[C]: PartialFunction[actionQueue, actionQueue] = raceToOutputs[C]

  /**
    * Preserves the order of the actions in which they are defined, but at first will give preference to loaders. If
    * there are no loaders, keeps the order.
    *
    * @tparam C
    * @return
    */
  def preferLoaders: PartialFunction[actionQueue, actionQueue] = takeLoaders orElse asInTheFlow

  /**
    * With Spark, waimak writers would usually force execution of the DAG and will produce outputs, while other waimak actions
    * could be preparing the steps of the DAG. In order to load the Spark Executors with work this strategy:
    *  1) will first choose only writers, as those are most likely to execute the DAG
    *  2) if there were no writers, it will choose only actions with inputs, as those will to DAG execution faster
    *  3) if there were no writers and actions with inputs leaves them as they are
    * @tparam C
    * @return
    */
  def raceToOutputs[C]: PartialFunction[actionQueue, actionQueue] = takeWriters orElse takeWithInputs orElse asInTheFlow

  /**
    * In order to race to actions that execute Spark DAG faster, it is needed to schedule certain actions earlier, regardless
    * in which order they are defined.
    * This function will first apply rules from raceToOutputs and than will sort the selected actions in the order of
    * labels defined by the 'orderedLabels' argument.
    *
    * @param orderedLabels
    * @tparam C
    * @return
    */
  def raceToOutputsAndThanSort(orderedLabels: Seq[String]): PartialFunction[actionQueue, actionQueue] = raceToOutputs andThen sortByOutputLabel(orderedLabels)

  /**
    * Preserves the order in which actions are defined in the flow.
    *
    * @tparam C
    * @return   same as input, no modifications
    */
  def asInTheFlow: PartialFunction[actionQueue, actionQueue] = new PartialFunction[actionQueue, actionQueue] {

    override def isDefinedAt(x: actionQueue): Boolean = true

    override def apply(v1: actionQueue): actionQueue = v1

  }

  private def takeWriters: PartialFunction[actionQueue, actionQueue] = new PartialFunction[actionQueue, actionQueue] {

    override def isDefinedAt(x: actionQueue): Boolean = x.exists(_.outputLabels.isEmpty)

    override def apply(v1: actionQueue): actionQueue = v1.filter(_.outputLabels.isEmpty)

  }

  private def takeLoaders: PartialFunction[actionQueue, actionQueue] = new PartialFunction[actionQueue, actionQueue] {

    override def isDefinedAt(x: actionQueue): Boolean = x.exists(_.inputLabels.isEmpty)

    override def apply(v1: actionQueue): actionQueue = v1.filter(_.inputLabels.isEmpty)

  }

  private def takeWithInputs: PartialFunction[actionQueue, actionQueue] = new PartialFunction[actionQueue, actionQueue] {

    override def isDefinedAt(x: actionQueue): Boolean = x.exists(_.inputLabels.nonEmpty)

    override def apply(v1: actionQueue): actionQueue = v1.filter(_.inputLabels.nonEmpty)

  }

  private def sortByOutputLabel(orderedLabels: Seq[String]): PartialFunction[actionQueue, actionQueue] = new PartialFunction[actionQueue, actionQueue] {

    val labelsPos: Map[String, Int] = orderedLabels.zipWithIndex.toMap

    override def isDefinedAt(x: actionQueue): Boolean = orderedLabels.nonEmpty && !x.exists(_.outputLabels.isEmpty)

    override def apply(v1: actionQueue): actionQueue = if (v1.isEmpty) v1 else {
      val parts = v1.partition(a => a.outputLabels.exists(labelsPos.contains))
      val sortedPart = parts._1.map(a => (a, a.outputLabels.filter(labelsPos.contains).map(labelsPos(_)).min)).sortWith(_._2 < _._2).map(_._1)
      sortedPart ++ parts._2
    }

  }
}