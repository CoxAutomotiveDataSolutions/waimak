package com.coxautodata.waimak.dataflow.spark

import com.coxautodata.waimak.dataflow._
import org.apache.spark.sql._


/**
  * Instances of this class build a bridge between OOP part of the Waimak engine and functional definition of the
  * data flow.
  *
  * Created by Alexei Perelighin on 03/11/17.
  *
  * @param inputLabels -
  * @param outputLabels
  * @param exec        - function to execute with inputs matched to input labels and outputs to output labels. By the time it
  *                    is called, all inputs were validated and action is in runnable flow state.
  * @tparam C the type of the context of the flow in which this action runs
  */
class SimpleAction[C](val inputLabels: List[String], val outputLabels: List[String]
                      , exec: DataFlowEntities => ActionResult) extends DataFlowAction[C] {

  /**
    * Perform the action. Puts inputs into a map and invokes the exec function.
    *
    * @param inputs the DataFlowEntities corresponding to the inputLabels
    * @return the action outputs (these must be declared in the same order as their labels in outputLabels)
    */
  override def performAction(inputs: DataFlowEntities, flowContext: C): ActionResult = {
    val params: DataFlowEntities = inputs.filterLabels(inputLabels)
    exec(params)
  }

}

/**
  * Spark specific simple action, that sets spark specific generics.
  *
  * @param inputLabels -
  * @param outputLabels
  * @param exec        - function to execute with inputs matched to input labels and outputs to output labels. By the time it
  *                    is called, all inputs were validated and action is in runnable flow state.
  * @param sqlTables
  */
class SparkSimpleAction(inputLabels: List[String], outputLabels: List[String]
                        , exec: DataFlowEntities => ActionResult
                        , val sqlTables: Seq[String]) extends SimpleAction[SparkFlowContext](inputLabels, outputLabels, exec) {

}

