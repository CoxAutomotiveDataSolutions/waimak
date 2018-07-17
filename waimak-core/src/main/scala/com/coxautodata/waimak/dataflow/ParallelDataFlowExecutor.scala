package com.coxautodata.waimak.dataflow

import java.util.concurrent.{BlockingQueue, Executors, LinkedBlockingQueue, TimeUnit}

import com.coxautodata.waimak.log.Logging

import scala.annotation.tailrec
import scala.util.Try
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
  * Created by Alexei Perelighin on 2018/05/28
  *
  * @tparam C
  */
class ParallelDataFlowExecutor[C]( numberOfParallelJobs: Int
                                      , override val flowReporter: FlowReporter[C]
                                      , override val priorityStrategy: Seq[DataFlowAction[C]] => Seq[DataFlowAction[C]])
  extends DataFlowExecutor[C] with Logging {

  type actionType = DataFlowAction[C]

  type futureResult = (actionType, Try[ActionResult])

  implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(numberOfParallelJobs))

  override def execute(dataFlow: DataFlow[C]): (Seq[DataFlowAction[C]], DataFlow[C]) = {
    /**
      * When actions finish, they post their ID into the queue, which is processed
      */
    val actionFinishedNotificationQueue: BlockingQueue[futureResult] = new LinkedBlockingQueue[futureResult]()

    //At the moment will schedule one task at a time, so the priority and next runnable will run far too often
    @tailrec
    def loop(currentFlow: DataFlow[C], running: Set[String], successfulActions: Seq[actionType]
             , futures: Seq[Future[futureResult]]): Try[(Seq[actionType], DataFlow[C])] = {
      val toSchedule: Option[DataFlowAction[C]] = priorityStrategy(currentFlow.nextRunnable(DEFAULT_POOL_NAME).filter(a => !running.contains(a.guid))).headOption
      (running.size >= numberOfParallelJobs, toSchedule) match {
        case (_, None) if running.isEmpty => {
          logInfo("Finished Action" + successfulActions)
          Success((successfulActions, currentFlow))
        }
        case (true, _) | (_, None) => {
          //in order to continue an action needs to finish
          Try {
            var finished: Option[futureResult] = None
            do {
              logInfo(s"Waiting for an action to finish to continue. Running actions: ${running.size}, is there are action to schedule: ${toSchedule.isDefined}")
              finished = Option(actionFinishedNotificationQueue.poll(1, TimeUnit.SECONDS))
            } while (!finished.isDefined)
            logInfo(s"No more waiting for an action. ${finished.isDefined}")
            //            println(s"No more waiting for an action. ${finished.isDefined}")
            finished
          } match {
            case Success(Some((action, Success(actionResult)))) => {
              //TODO: add optimistic non blocking drain of the queue to speed up the
              logInfo(s"An action has completed. ${action.logLabel}")
              val newSubmitted = running.filterNot(_ == action.guid)
              val nextFlowState = currentFlow.executed(action, actionResult)
              val newSuccessfulActions = successfulActions :+ action
              loop(nextFlowState, newSubmitted, newSuccessfulActions, futures)
            }
            case Success(Some((action, Failure(exception)))) => {
              logError(s"Action failed: ${action.logLabel}.", exception)
              Failure(exception)
            }
            case Success(None) => Failure(new DataFlowException(s"Something when wrong!!! Not sure what happend."))
            case Failure(e) => Failure(e)
          }
        }
        case (_, Some(action)) => {
          // lets do some scheduling
          logInfo(s"Submitting action: ${action.logLabel}. runnung.size = ${running.size}")
          val ft: Future[futureResult] = scheduleAction(currentFlow, action).map{r => actionFinishedNotificationQueue.offer(r); r}
          loop(currentFlow, running + action.guid, successfulActions, futures :+ ft)
        }
      }
    }

    val preparedDataFlow = dataFlow.prepareForExecution()

    val executionResults: Try[(Seq[actionType], DataFlow[C])] = loop(preparedDataFlow, Set.empty, Seq.empty, Seq.empty)
    //    executionResults.foreach(p => println(p))
    //TODO: In the future the Executor trait needs to return Try. Also there are a lot of options to
    //implement restartability/recovery, which need to think through
    //for now this is left as it is, as in most of the existing cases it is fine for the Flow to fail and if
    // data has been commited to permament folders by using Waimak libs, it is possible to remove that partially
    //committed state from storage.
    //The bellow cod is BAD, will be fixed in the future
    executionResults.get
  }

  def scheduleAction(currentFlow: DataFlow[C], action: actionType): Future[futureResult] = {
    val inputEntities: DataFlowEntities = {
      currentFlow.inputs.filterLabels(action.inputLabels)
    }
    val ft = Future[futureResult] {
      (action, action.performAction(inputEntities, currentFlow.flowContext))
    }
    ft
  }

  override def initActionScheduler(): ActionScheduler[C] = ???
}

object ParallelDataFlowExecutor {

  def noPreference[C](actions: Seq[DataFlowAction[C]]): Seq[DataFlowAction[C]] = actions

}
