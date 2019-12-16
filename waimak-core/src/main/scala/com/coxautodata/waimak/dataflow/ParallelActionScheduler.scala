package com.coxautodata.waimak.dataflow

import java.util
import java.util.concurrent.{BlockingQueue, Executors, LinkedBlockingQueue, TimeUnit}

import com.coxautodata.waimak.log.Logging

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
  * Can run multiple actions in parallel with multiple execution pools support.
  *
  * It was originally designed to benefit from Spark fair scheduler https://spark.apache.org/docs/latest/job-scheduling.html#fair-scheduler-pools
  * Execution pool names must be the same as the Fair Scheduler pool names and number of parallel jobs within a pool is the number of Java threads.
  *
  * Example:
  * to configure 2 pools for Spark Fair Scheduler following xml must be passed to spark :
  *
  * <?xml version="1.0"?>
  * <allocations>
  * <pool name="high">
  * <schedulingMode>FAIR</schedulingMode>
  * <weight>1000</weight>
  * <minShare>0</minShare>
  * </pool>
  * <pool name="medium">
  * <schedulingMode>FAIR</schedulingMode>
  * <weight>25</weight>
  * <minShare>0</minShare>
  * </pool>
  * </allocations>
  *
  * following configuration options need to be specified:
  *   1. spark.scheduler.mode=FAIR
  *   2. spark.scheduler.allocation.file=PATH_TO_THE_XML
  *
  * in code pools parameter:
  * Map( ("high" -> ExecutionPoolDesc("high", 10, Set.Empty, None)), ("medium" -> ExecutionPoolDesc("medium", 20, Set.Empty, None)) )
  *
  * Created by Alexei Perelighin on 2018/07/10
  *
  * @param pools                           details of the execution pools: name, limits, running actions, thread pool
  * @param actionFinishedNotificationQueue thread safe queue through which threads that have finished actions will communicate back to the scheduler
  */
class ParallelActionScheduler(val pools: Map[String, ExecutionPoolDesc]
                              , val actionFinishedNotificationQueue: BlockingQueue[(String, DataFlowAction, Try[ActionResult])]
                             )
  extends ActionScheduler with Logging {

  type actionType = DataFlowAction

  type futureResult = (String, actionType, Try[ActionResult])

  override def dropRunning(poolNames: Set[String], from: Seq[DataFlowAction]): Seq[DataFlowAction] = {
    if (from.isEmpty) from
    else {
      val running = pools.filterKeys(poolNames.contains).values.flatMap(_.running).toSet
      from.filter(a => !running.contains(a.schedulingGuid))
    }
  }

  override def hasRunningActions: Boolean = pools.exists(kv => kv._2.running.nonEmpty)


  override def waitToFinish(flowContext: FlowContext, flowReporter: FlowReporter): (ActionScheduler, Seq[(DataFlowAction, Try[ActionResult])]) = {
    @tailrec
    def loop(finished: Option[Seq[futureResult]]): (ActionScheduler, Seq[(DataFlowAction, Try[ActionResult])]) = {
      finished match {
        case None =>
          val runningActionsCount = pools.map(_._2.running.size).sum
          logInfo(s"Waiting for an action to finish to continue. Running actions: $runningActionsCount")
          loop(Option(actionFinishedNotificationQueue.poll(1, TimeUnit.MINUTES))
            .map { oneAction =>
              //optimistic attempt to get results for more actions without blocking
              val bucket = new util.LinkedList[futureResult]()
              actionFinishedNotificationQueue.drainTo(bucket, 1000)
              bucket.asScala :+ oneAction
            }
          )
        case Some(rSet) =>
          val poolActionGuids: Map[String, Set[String]] = rSet.map(r => (r._1, r._2.schedulingGuid)).groupBy(_._1).mapValues(v => v.map(_._2).toSet)
          poolActionGuids.foreach(kv => logDebug("waitToFinish finished: " + kv._1 + " " + kv._2.mkString("[", ",", "]")))
          val newPools = poolActionGuids.foldLeft(pools) { (newPools, kv) =>
            val newPoolDesc = newPools(kv._1).removeActionGUIDS(kv._2)
            newPools + (kv._1 -> newPoolDesc)
          }
          (new ParallelActionScheduler(newPools, actionFinishedNotificationQueue), rSet.map(r => (r._2, r._3)))
      }
    }

    loop(None)
  }

  override def schedule(poolName: String, action: DataFlowAction, entities: DataFlowEntities, flowContext: FlowContext, flowReporter: FlowReporter): ActionScheduler = {
    logInfo("Scheduling Action: " + poolName + " : " + action.schedulingGuid + " : " + action.logLabel)
    val poolDesc = pools.getOrElse(poolName, ExecutionPoolDesc(poolName, 1, Set.empty, None)).ensureRunning()

    Future[futureResult] {
      flowReporter.reportActionStarted(action, flowContext)
      logInfo("Executing action " + actionFinishedNotificationQueue.size() + " " + action.logLabel)
      flowContext.setPoolIntoContext(poolName)
      val actionResult = {
        // Cannot use Try here as Try does not catch fatal exceptions
        try {
          action.performAction(entities, flowContext)
        } catch {
          case NonFatal(e) => throw e
          //Wrap fatal exceptions in non-fatal exception
          case e: Throwable => throw new DataFlowException("Fatal exception performing action", e)
        }
      }
      (poolName, action, actionResult)
    }(poolDesc.threadsExecutor.get)
      .onComplete {
        case Success(res) =>
          actionFinishedNotificationQueue.offer(res)
          flowReporter.reportActionFinished(action, flowContext)
        case Failure(e) =>
          actionFinishedNotificationQueue.offer((poolName, action, Failure(e)))
      }(scala.concurrent.ExecutionContext.global)

    logInfo("Submitted to Pool Action: " + poolName + " : " + action.schedulingGuid + " : " + action.logLabel)
    val newPoolDesc = poolDesc.addActionGUID(action.schedulingGuid)
    new ParallelActionScheduler(pools + (poolName -> newPoolDesc), actionFinishedNotificationQueue)
  }

  override def availableExecutionPools: Option[Set[String]] = {
    val res = Option(pools.filter(kv => kv._2.running.size < kv._2.maxJobs).keySet).filter(_.nonEmpty)
    logInfo("availableExecutionPools: " + res.map(_.mkString("[", ",", "]")))
    res
  }

  override def shutDown(): Try[ActionScheduler] = {
    logInfo("ParallelScheduler.close")
    Try {
      val newPools = pools.map(kv => (kv._1, kv._2.shutdown()))
      new ParallelActionScheduler(newPools, actionFinishedNotificationQueue)
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

  def apply(): ParallelActionScheduler = apply(1)

  def apply(maxJobs: Int): ParallelActionScheduler = apply(Map(DEFAULT_POOL_NAME -> maxJobs))

  def apply(poolsSpec: Map[String, Int]): ParallelActionScheduler = {
    val pools = poolsSpec.map(kv => (kv._1, ExecutionPoolDesc(kv._1, kv._2, Set.empty, None)))
    new ParallelActionScheduler(pools, new LinkedBlockingQueue[(String, DataFlowAction, Try[ActionResult])]())
  }

}