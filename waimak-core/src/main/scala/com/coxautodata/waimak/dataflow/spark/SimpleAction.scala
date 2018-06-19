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
  * @tparam T the type of the entity which we are transforming (e.g. Dataset)
  * @tparam C the type of the context of the flow in which this action runs
  */
class SimpleAction[T, C](val inputLabels: List[String], val outputLabels: List[String]
                         , exec: Map[String, T] => ActionResult[T]) extends DataFlowAction[T, C] {

  /**
    * Perform the action. Puts inputs into a map and invokes the exec function.
    *
    * @param inputs the DataFlowEntities corresponding to the inputLabels
    * @return the action outputs (these must be declared in the same order as their labels in outputLabels)
    */
  override def performAction(inputs: DataFlowEntities[T], flowContext: C): ActionResult[T] = {
    val params: Map[String, T] = inputs.filterLabels(inputLabels).entities
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
                        , exec: Map[String, Dataset[_]] => ActionResult[Dataset[_]]
                        , val sqlTables: Seq[String]) extends SimpleAction[Dataset[_]
  , SparkFlowContext](inputLabels, outputLabels, exec) {

}

