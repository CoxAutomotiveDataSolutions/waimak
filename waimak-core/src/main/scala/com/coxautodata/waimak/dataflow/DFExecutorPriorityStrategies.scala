package com.coxautodata.waimak.dataflow

/**
  * Defines various priority strategies for [[ DataFlowExecutor ]] to use.
  *
  * Created by Alexei Perelighin on 24/08/2018.
  */
object DFExecutorPriorityStrategies {

  type actionQueue[C] = Seq[DataFlowAction[C]]

  type priorityStrategy[C] = actionQueue[C] => actionQueue[C]

  /**
    * Default strategy, at the moment it will not do anything.
    *
    * @tparam C
    * @return
    */
  def defaultPriorityStrategy[C]: PartialFunction[actionQueue[C], actionQueue[C]] = raceToOutputs[C]

  /**
    * Preserves the order of the actions in which they are defined, but at first will give preference to loaders. If
    * there are no loaders, keeps the order.
    *
    * @tparam C
    * @return
    */
  def preferLoaders[C]: PartialFunction[actionQueue[C], actionQueue[C]] = takeLoaders[C] orElse asInTheFlow[C]

  /**
    * With Spark, waimak writers would usually force execution of the DAG and will produce outputs, while other waimak actions
    * could be preparing the steps of the DAG. In order to load the Spark Executors with work this strategy:
    *  1) will first choose only writers, as those are most likely to execute the DAG
    *  2) if there were no writers, it will choose only actions with inputs, as those will to DAG execution faster
    *  3) if there were no writers and actions with inputs leaves them as they are
    * @tparam C
    * @return
    */
  def raceToOutputs[C]: PartialFunction[actionQueue[C], actionQueue[C]] = takeWriters[C] orElse takeWithInputs[C] orElse asInTheFlow[C]

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
  def raceToOutputsAndThanSort[C](orderedLabels: Seq[String]): PartialFunction[actionQueue[C], actionQueue[C]] = raceToOutputs[C] andThen sortByOutputLabel[C](orderedLabels)

  /**
    * Preserves the order in which actions are defined in the flow.
    *
    * @tparam C
    * @return   same as input, no modifications
    */
  def asInTheFlow[C]: PartialFunction[actionQueue[C], actionQueue[C]] = new PartialFunction[actionQueue[C], actionQueue[C]] {

    override def isDefinedAt(x: actionQueue[C]): Boolean = true

    override def apply(v1: actionQueue[C]): actionQueue[C] = v1

  }

  private def takeWriters[C]: PartialFunction[actionQueue[C], actionQueue[C]] = new PartialFunction[actionQueue[C], actionQueue[C]] {

    override def isDefinedAt(x: actionQueue[C]): Boolean = x.exists(_.outputLabels.isEmpty)

    override def apply(v1: actionQueue[C]): actionQueue[C] = v1.filter(_.outputLabels.isEmpty)

  }

  private def takeLoaders[C]: PartialFunction[actionQueue[C], actionQueue[C]] = new PartialFunction[actionQueue[C], actionQueue[C]] {

    override def isDefinedAt(x: actionQueue[C]): Boolean = x.exists(_.inputLabels.isEmpty)

    override def apply(v1: actionQueue[C]): actionQueue[C] = v1.filter(_.inputLabels.isEmpty)

  }

  private def takeWithInputs[C]: PartialFunction[actionQueue[C], actionQueue[C]] = new PartialFunction[actionQueue[C], actionQueue[C]] {

    override def isDefinedAt(x: actionQueue[C]): Boolean = x.exists(_.inputLabels.nonEmpty)

    override def apply(v1: actionQueue[C]): actionQueue[C] = v1.filter(_.inputLabels.nonEmpty)

  }

  private def sortByOutputLabel[C](orderedLabels: Seq[String]): PartialFunction[actionQueue[C], actionQueue[C]] = new PartialFunction[actionQueue[C], actionQueue[C]] {

    val labelsPos: Map[String, Int] = orderedLabels.zipWithIndex.toMap

    override def isDefinedAt(x: actionQueue[C]): Boolean = orderedLabels.nonEmpty && !x.exists(_.outputLabels.isEmpty)

    override def apply(v1: actionQueue[C]): actionQueue[C] = if (v1.isEmpty) v1 else {
      val parts = v1.partition(a => a.outputLabels.exists(labelsPos.contains))
      val sortedPart = parts._1.map(a => (a, a.outputLabels.filter(labelsPos.contains).map(labelsPos(_)).min)).sortWith(_._2 < _._2).map(_._1)
      sortedPart ++ parts._2
    }

  }
}