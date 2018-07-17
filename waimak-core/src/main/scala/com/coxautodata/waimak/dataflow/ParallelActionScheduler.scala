package com.coxautodata.waimak.dataflow

import java.util.concurrent.Executors

import com.coxautodata.waimak.log.Logging

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.Try

/**
  * Created by Alexei Perelighin on 2018/07/10
  *
  * @param pools
  * @tparam C
  */
class ParallelActionScheduler[C](pools: Map[String, ExecutionPoolDesc]) extends ActionScheduler[C] with Logging {

  type actionType = DataFlowAction[C]

  type futureResult = (actionType, Try[ActionResult])

  override def dropRunning(poolName: String, from: Seq[DataFlowAction[C]]): Seq[DataFlowAction[C]] = pools.get(poolName).fold(from)(epd =>
    from.filter(a => !epd.running.contains(a.guid))
  )

  override def hasRunningActions(): Boolean = pools.exists(kv => kv._2.running.nonEmpty)

  override def waitToFinish(): Try[(ActionScheduler[C], Seq[(DataFlowAction[C], Try[ActionResult])])] = ???

  override def submitAction(poolName: String, action: DataFlowAction[C], entities: DataFlowEntities, flowContext: C): ActionScheduler[C] = ???

  override def availableExecutionPool(): Option[String] = pools.find(kv => kv._2.running.nonEmpty).map(_._1)

}

case class ExecutionPoolDesc(poolName: String, maxJobs: Int, running: Set[String], threadsExecutor: ExecutionContextExecutor)

object ParallelActionScheduler {

  def apply[C](poolsSpec: Map[String, Int]): ParallelActionScheduler[C] = {
    val pools = poolsSpec.map( kv => (kv._1, ExecutionPoolDesc(kv._1, kv._2, Set.empty, ExecutionContext.fromExecutor(Executors.newFixedThreadPool(kv._2)))))
    new ParallelActionScheduler[C](pools)
  }

}