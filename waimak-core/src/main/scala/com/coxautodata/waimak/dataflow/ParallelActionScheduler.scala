package com.coxautodata.waimak.dataflow

import java.util
import java.util.concurrent.{BlockingQueue, Executors, LinkedBlockingQueue, TimeUnit}

import com.coxautodata.waimak.log.Logging

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._

/**
  * Created by Alexei Perelighin on 2018/07/10
  *
  * @param pools
  * @tparam C
  */
class ParallelActionScheduler[C](val pools: Map[String, ExecutionPoolDesc], val actionFinishedNotificationQueue: BlockingQueue[(String, DataFlowAction[C], Try[ActionResult])])
  extends ActionScheduler[C] with Logging {

  type actionType = DataFlowAction[C]

  type futureResult = (String, actionType, Try[ActionResult])

  override def dropRunning(poolName: String, from: Seq[DataFlowAction[C]]): Seq[DataFlowAction[C]] = pools.get(poolName).fold(from)(epd =>
    from.filter(a => !epd.running.contains(a.guid))
  )

  override def hasRunningActions(): Boolean = pools.exists(kv => kv._2.running.nonEmpty)

  override def waitToFinish(): Try[(ActionScheduler[C], Seq[(DataFlowAction[C], Try[ActionResult])])] = {
    Try {
      var finished: Option[Seq[futureResult]] = None
      do {
        val runningActionsCount = pools.map(_._2.running.size).sum
        logInfo(s"Waiting for an action to finish to continue. Running actions: ${runningActionsCount}")
        finished = Option(actionFinishedNotificationQueue.poll(1, TimeUnit.SECONDS))
            .map { oneAction =>
              //optimistic attempt to get results for more actions without blocking
              val bucket = new util.LinkedList[futureResult]()
              actionFinishedNotificationQueue.drainTo(bucket, 1000)
              bucket.asScala :+ oneAction
            }
      } while (!finished.isDefined)
      logInfo(s"No more waiting for an action. ${finished.isDefined}")
      finished.map { rSet =>
        val poolActionGuids: Map[String, Set[String]] = rSet.map(r => (r._1, r._2.guid)).groupBy(_._1).mapValues(v => v.map(_._2).toSet)
        val newPools = poolActionGuids.foldLeft(pools) { (newPools, kv) =>
          val newPoolDesc = newPools(kv._1).removeActionGUIDS(kv._2)
          newPools + (kv._1 -> newPoolDesc)
        }
        (new ParallelActionScheduler(newPools, actionFinishedNotificationQueue), rSet.map(r => (r._2, r._3)))
      }
    }.flatMap(_ match {
      case Some(r) => Success(r)
      case _ => Failure(new DataFlowException(s"Something when wrong!!! Not sure what happend."))
    }
    )
  }

  override def submitAction(poolName: String, action: DataFlowAction[C], entities: DataFlowEntities, flowContext: C): ActionScheduler[C] = {
    val poolDesc = pools.getOrElse(poolName, ExecutionPoolDesc(poolName, 1, Set.empty, ExecutionContext.fromExecutor(Executors.newFixedThreadPool(3))))
    val ft = Future[futureResult] {
      (poolName, action, action.performAction(entities, flowContext))
    }(poolDesc.threadsExecutor)
    val newPoolDesc = poolDesc.addActionGUID(action.guid)
    new ParallelActionScheduler(pools + (poolName -> newPoolDesc), actionFinishedNotificationQueue)
  }

  override def availableExecutionPool(): Option[String] = pools.find(kv => kv._2.running.size < kv._2.maxJobs).map(_._1)

}

case class ExecutionPoolDesc(poolName: String, maxJobs: Int, running: Set[String], threadsExecutor: ExecutionContextExecutor) {

   def addActionGUID(guid: String): ExecutionPoolDesc = ExecutionPoolDesc(poolName, maxJobs, running + guid, threadsExecutor)

   def removeActionGUIDS(guids: Set[String]): ExecutionPoolDesc = ExecutionPoolDesc(poolName, maxJobs, running &~ guids, threadsExecutor)

}

object ParallelActionScheduler {

  def apply[C](): ParallelActionScheduler[C] = apply(1)

  def apply[C](maxJobs: Int): ParallelActionScheduler[C] = apply(Map(DEFAULT_POOL_NAME -> maxJobs))

  def apply[C](poolsSpec: Map[String, Int]): ParallelActionScheduler[C] = {
    val pools = poolsSpec.map( kv => (kv._1, ExecutionPoolDesc(kv._1, kv._2, Set.empty, ExecutionContext.fromExecutor(Executors.newFixedThreadPool(kv._2 + 3)))))
    new ParallelActionScheduler[C](pools, new LinkedBlockingQueue[(String, DataFlowAction[C], Try[ActionResult])]())
  }

}