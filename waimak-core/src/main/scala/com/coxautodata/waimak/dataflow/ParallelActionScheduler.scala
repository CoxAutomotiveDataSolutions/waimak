package com.coxautodata.waimak.dataflow

import java.util
import java.util.concurrent.{BlockingQueue, Executors, LinkedBlockingQueue, TimeUnit}

import com.coxautodata.waimak.log.Logging

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._

/**
  * Created by Alexei Perelighin on 2018/07/10
  *
  * @param pools
  * @tparam C
  */
class ParallelActionScheduler[C](val pools: Map[String, ExecutionPoolDesc]
                                 , val actionFinishedNotificationQueue: BlockingQueue[(String, DataFlowAction[C], Try[ActionResult])]
                                , val poolIntoContext: (String, C) => Unit
                                )
  extends ActionScheduler[C] with Logging {

  type actionType = DataFlowAction[C]

  type futureResult = (String, actionType, Try[ActionResult])

  override def dropRunning(poolNames: Set[String], from: Seq[DataFlowAction[C]]): Seq[DataFlowAction[C]] = {
    logInfo("dropRunning from:")
    from.foreach(a => logInfo(a.schedulingGuid + " " + a.logLabel)) //DEBUG msgs, to remove after investigations
    if (from.isEmpty) from
    else {
      val running = pools.filterKeys(poolNames.contains).values.flatMap(_.running).toSet
      logInfo("dropRunning running:")
      running.foreach(a => logInfo("running: " + a)) //DEBUG msgs, to remove after investigations
      from.filter(a => !running.contains(a.schedulingGuid))
    }
  }

  override def hasRunningActions: Boolean = pools.exists(kv => kv._2.running.nonEmpty)

  override def waitToFinish(): Try[(ActionScheduler[C], Seq[(DataFlowAction[C], Try[ActionResult])])] = {
    Try {
      var finished: Option[Seq[futureResult]] = None
      do {
        val runningActionsCount = pools.map(_._2.running.size).sum
        logInfo(s"Waiting for an action to finish to continue. Running actions: $runningActionsCount")
        finished = Option(actionFinishedNotificationQueue.poll(1, TimeUnit.MINUTES))
          .map { oneAction =>
            //optimistic attempt to get results for more actions without blocking
            val bucket = new util.LinkedList[futureResult]()
            actionFinishedNotificationQueue.drainTo(bucket, 1000)
            bucket.asScala :+ oneAction
          }
      } while (finished.isEmpty)
      logInfo(s"No more waiting for an action. ${finished.isDefined}")
      finished.map { rSet =>
        val poolActionGuids: Map[String, Set[String]] = rSet.map(r => (r._1, r._2.schedulingGuid)).groupBy(_._1).mapValues(v => v.map(_._2).toSet)
        logInfo("waitToFinish:")
        poolActionGuids.foreach(kv => logInfo("waitToFinish finished: " + kv._1 + " " + kv._2.mkString("[", ",", "]")))
        val newPools = poolActionGuids.foldLeft(pools) { (newPools, kv) =>
          val newPoolDesc = newPools(kv._1).removeActionGUIDS(kv._2)
          newPools + (kv._1 -> newPoolDesc)
        }
        (new ParallelActionScheduler(newPools, actionFinishedNotificationQueue, poolIntoContext), rSet.map(r => (r._2, r._3)))
      }
    }.flatMap {
      case Some(r) => Success(r)
      case _ => Failure(new DataFlowException(s"Something when wrong!!! Not sure what happend."))
    }
  }

  override def submitAction(poolName: String, action: DataFlowAction[C], entities: DataFlowEntities, flowContext: C): ActionScheduler[C] = {
    logInfo("Submitting Action: " + poolName + " : " + action.schedulingGuid + " : " + action.logLabel)
    val poolDesc = pools.getOrElse(poolName, ExecutionPoolDesc(poolName, 1, Set.empty, None)).ensureRunning()
    val ft = Future[futureResult] {
      logInfo("Executing action " + actionFinishedNotificationQueue.size() + " " + action.logLabel)
      poolIntoContext(poolName, flowContext)
      val actionResult = Try(action.performAction(entities, flowContext)).flatten
      val res = (poolName, action, actionResult)
      actionFinishedNotificationQueue.offer(res)
      res
    }(poolDesc.threadsExecutor.get) // ensureRunning made sure that this is a Some
    logInfo("Submitted to Pool Action: " + poolName + " : " + action.schedulingGuid + " : " +  action.logLabel)
    val newPoolDesc = poolDesc.addActionGUID(action.schedulingGuid)
    new ParallelActionScheduler(pools + (poolName -> newPoolDesc), actionFinishedNotificationQueue, poolIntoContext)
  }

  override def availableExecutionPools(): Option[Set[String]] = {
    val res = Option(pools.filter(kv => kv._2.running.size < kv._2.maxJobs).keySet).filter(_.nonEmpty)
    logInfo("availableExecutionPools: " + res.map(_.mkString("[", ",", "]")))
    res
  }

  override def shutDown(): Try[ActionScheduler[C]] = {
    logInfo("ParallelScheduler.close")
    Try {
      val newPools = pools.mapValues(_.shutdown())
      new ParallelActionScheduler(newPools, actionFinishedNotificationQueue, poolIntoContext)
    }
  }

}

case class ExecutionPoolDesc(poolName: String, maxJobs: Int, running: Set[String], threadsExecutor: Option[ExecutionContextExecutorService]) {

  def addActionGUID(guid: String): ExecutionPoolDesc = ExecutionPoolDesc(poolName, maxJobs, running + guid, threadsExecutor)

  def removeActionGUIDS(guids: Set[String]): ExecutionPoolDesc = ExecutionPoolDesc(poolName, maxJobs, running &~ guids, threadsExecutor)

  def shutdown(): ExecutionPoolDesc = threadsExecutor.fold(this) { exec =>
    exec.shutdown()
    ExecutionPoolDesc(poolName, maxJobs, running, None)
  }

  def ensureRunning(): ExecutionPoolDesc = threadsExecutor.fold(ExecutionPoolDesc(poolName, maxJobs, running, Some(ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(maxJobs)))))(_ => this)

}

object ParallelActionScheduler {

  def apply[C]()(poolIntoContext: (String, C) => Unit): ParallelActionScheduler[C] = apply(1)(poolIntoContext)

  def apply[C](maxJobs: Int)(poolIntoContext: (String, C) => Unit): ParallelActionScheduler[C] = apply(Map(DEFAULT_POOL_NAME -> maxJobs))(poolIntoContext)

  def apply[C](poolsSpec: Map[String, Int])(poolIntoContext: (String, C) => Unit): ParallelActionScheduler[C] = {
    val pools = poolsSpec.map( kv => (kv._1, ExecutionPoolDesc(kv._1, kv._2, Set.empty, None)))
    new ParallelActionScheduler[C](pools, new LinkedBlockingQueue[(String, DataFlowAction[C], Try[ActionResult])](), poolIntoContext)
  }

  def noPool[C](p: String, c: C): Unit = ()

}