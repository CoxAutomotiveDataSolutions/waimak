package com.coxautodata.waimak.azure.table

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Future => _, TimeoutException => _, _}

import com.coxautodata.waimak.log.Logging
import com.microsoft.azure.storage.{CloudStorageAccount, StorageException}
import com.microsoft.azure.storage.table.{DynamicTableEntity, TableBatchOperation}

import scala.concurrent.duration.Duration
import scala.concurrent._
import scala.util.{Failure, Success, Try}

/**
  * Azure tables are accessed using HTTP/HTTPS, with maximum batch size of 100 records per Azure Table partition key.
  * Therefore there is a lot of lag between the batches and spending one spark executor per connection will be very
  * wasteful. To work around it, this class will open multiple threads each having its own connection to an Azure Table
  * and will push data from the blocking queue.
  *
  * Created by Alexei Perelighin on 2018/03/01
  *
  * @param table        - Azure Table name
  * @param connection   - Azure Table connection string
  * @param threadNum    - Number of threads/connections.
  * @param timeoutMs    - Timeout in milliseconds for an Azure Table operation
  * @param retryDelayMs - Delay in milliseconds to wait between successive retries of an Azure Table operation
  *                     in case of failure or timeout. Number of elements in the sequence indicates how number of
  *                     retries for the operation.
  */
class AzureTableMultiWriter(val table: String, val connection: String, val threadNum: Int,
                            val timeoutMs: Long, val retryDelayMs: Seq[Long]) extends Logging {

  /**
    * Multiple data producers will push pages of at most 100 records into the queue. There will be no validation
    * of the pages size by this class, so code that produces pages must take care of that.
    */
  val queue: BlockingQueue[Seq[DynamicTableEntity]] = new LinkedBlockingQueue[Seq[DynamicTableEntity]](threadNum * 2)

  protected val thereWillBeMoreData: AtomicBoolean = new AtomicBoolean(true)

  val threadFailed: AtomicBoolean = new AtomicBoolean(false)

  val threadPool: ExecutorService = Executors.newFixedThreadPool(threadNum)

  protected var futures: Seq[Future[Int]] = _

  /**
    * Initialises the writer threads, which will wait for pages of records to be placed into the queue.
    *
    * This method does not wait for completion and does not block, exits immediately after initialisation.
    */
  def run(): Unit = {

    implicit val ex: ExecutionContextExecutor = ExecutionContext.fromExecutor(threadPool)

    futures = (0 until threadNum).map { _ =>
      Future[Int] {
        val singleExecutor = Executors.newFixedThreadPool(1)
        val threadFutureExecutor: ExecutionContextExecutor = ExecutionContext.fromExecutor(singleExecutor)
        val storage = CloudStorageAccount.parse(connection)
        val tblClient = storage.createCloudTableClient
        val cloudTable = tblClient.getTableReference(table)

        var cnt = 0
        while ((queue.size() > 0 || thereWillBeMoreData.get()) && !threadFailed.get()) {
          val page = queue.poll(1, TimeUnit.SECONDS)
          if (page != null) {
            cnt += page.size
            val batchInsert = page.foldLeft(new TableBatchOperation()) { (batch, de) => batch.insertOrReplace(de); batch }
            val f = () => Future(cloudTable.execute(batchInsert))(threadFutureExecutor)
            Try(retry(f, timeoutMs, retryDelayMs)) match {
              case Success(_) =>
              case Failure(e) =>
                singleExecutor.shutdown()
                throw e
            }
          }
        }
        singleExecutor.shutdown()
        cnt
      }
    }
  }

  /**
    *
    * @param f         - Function that produces a Future
    * @param timeoutMs - Timeout in milliseconds for the future to complete
    * @param delaysMs  - Delay in milliseconds to wait between successive retries of the future
    *                  in case of failure or timeout. Number of elements in the sequence indicates how number of
    *                  retries for the future.
    * @tparam T - Return type of the future
    * @return
    */
  def retry[T](f: () => Future[T], timeoutMs: Long, delaysMs: Seq[Long]): T = {
    Try(Await.result(f(), Duration(timeoutMs, TimeUnit.MILLISECONDS))) recover {
      case e@(_: TimeoutException | _: StorageException) if delaysMs.nonEmpty =>
        logWarning(s"Task failed with exception. Will wait ${delaysMs.head} Ms and try again. " +
          s"${delaysMs.tail.length} attempts left after this retry.", e)
        Thread.sleep(timeoutMs)
        retry(f, timeoutMs, delaysMs.tail)
    } match {
      case Success(v) => v
      case Failure(e) =>
        threadFailed.set(true)
        throw e
    }
  }

  /**
    * When there will be no more data pushed into the queue, producer must call it. It will wait for the threads to
    * push all of the pages into the Azure Table and for all threads to finish.
    *
    * @return - number of records pushed by all threads
    */
  def finish: Int = {
    thereWillBeMoreData.set(false)
    val total = futures.map(f => Await.result(f, Duration.Inf)).sum
    threadPool.shutdown()
    total
  }

}
