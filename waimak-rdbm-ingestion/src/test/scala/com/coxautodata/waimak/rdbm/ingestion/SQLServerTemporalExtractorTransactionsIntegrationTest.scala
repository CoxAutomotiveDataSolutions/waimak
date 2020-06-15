package com.coxautodata.waimak.rdbm.ingestion

import java.sql.Timestamp
import java.time.Instant

import com.coxautodata.waimak.dataflow.Waimak
import com.coxautodata.waimak.dataflow.spark.SparkAndTmpDirSpec
import com.coxautodata.waimak.rdbm.ingestion.RDBMIngestionActions._
import monix.eval.Task
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.max
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.collection.immutable
import scala.collection.mutable.MutableList

/**
 * Test that the temporal extractor works correctly in the face of transactions
 *
 * Created by James Fielder on 2020/06/09
 */
class SQLServerTemporalExtractorTransactionsIntegrationTest
  extends SparkAndTmpDirSpec
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with SQLServerTemporalExtractorBase {

  import SQLServerTemporalExtractorBase._

  override val appName: String = "SQLServerTemporalConnectorTransactionsIntegrationTest"

  override def beforeAll(): Unit = {
    super.beforeEach()
    cleanupTables()
    setupTables()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()

    executeSQl(Seq("""if exists (select * from information_schema.tables where table_name = N'TestTemporal')
                     |begin
                     |    alter table TestTemporal set (SYSTEM_VERSIONING = OFF)
                     |    truncate table TestTemporal
                     |    truncate table TestTemporalHistory
                     |    alter table TestTemporal set (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.TestTemporalHistory))
                     |end""".stripMargin))
  }

  override def afterAll(): Unit = {
    super.afterEach()
//    cleanupTables()
  }

  describe("SQLServerTemporalExtractor") {
    it("should do basic inserts and deletes with DSL") {
      import ActionInterpreter._
      import monix.execution.Scheduler.Implicits.global
      val actions = MutableList[AppliedAction]()

//      val inserts: Seq[Add] = Seq(
//        Add(TestTemporal(1, "test", 0)),
//        Add(TestTemporal(None, "hi", 0))
//      )

      val inserts: Seq[Add] = (1 to 10).map(id => Add(TestTemporal(id, s"test${id}", 0))).toSeq
//
      actions ++= runActions(inserts).runSyncUnsafe()
    }

    it("should handle start/stop delta logic for the temporal tables") {
      val spark = sparkSession
      import spark.implicits._

      addNonTemporalTestData()
      addTemporalTestData()

      val sqlServerExtractor = new SQLServerTemporalExtractor(sparkSession, sqlServerConnectionDetails)
      val flow = Waimak.sparkFlow(sparkSession)
      val executor = Waimak.sparkExecutor()

      val tableConfig: Map[String, RDBMExtractionTableConfig] = Map("testtemporal" -> RDBMExtractionTableConfig("testtemporal", maxRowsPerPartition = Some(2)))


      val writeFlow = flow.extractToStorageFromRDBM(sqlServerExtractor
        , "dbo"
        , s"$testingBaseDir/output"
        , tableConfig
        , insertDateTime)("testtemporal")

      executor.execute(writeFlow)


      val deletes = "delete from TestTemporal where TestTemporalId = 2;"
      val inserts =
        s"""SET IDENTITY_INSERT TestTemporal ON;
           |insert into TestTemporal (TestTemporalID, TestTemporalValue) VALUES (8, 'Value8');
           |insert into TestTemporal (TestTemporalID, TestTemporalValue) VALUES (9, 'Value9');
           |SET IDENTITY_INSERT TestTemporal OFF;
           |""".stripMargin
      val updates =
        s"""
           |update TestTemporal set TestTemporalValue = 'New Value 5' where TestTemporalID = 5;
           |update TestTemporal set TestTemporalValue = 'New Value 6' where TestTemporalID = 6;
           |update TestTemporal set TestTemporalValue = 'New Value 7' where TestTemporalID = 7;
       """.stripMargin

      executeSQl(Seq(updates, inserts, deletes))

      val deltaWriteFlow = flow.extractToStorageFromRDBM(sqlServerExtractor
        , "dbo"
        , s"$testingBaseDir/output"
        , tableConfig
        , insertDateTime)("testtemporal")

      val res = executor.execute(deltaWriteFlow)

      val testTemporal = res._2.inputs.get[Dataset[_]]("testtemporal")
      testTemporal.sort("TestTemporalID").show()
      testTemporal.sort("source_type", "testtemporalid")
        .as[TestTemporal].collect()
        //For some reason, sometimes (not consistently) > seems to act like >= on these datetime2 fields so we need to filter
        //out the records which could mess up our test
        .filterNot(_.testtemporalid.contains(1)) should be(Seq(
        TestTemporal(5, "New Value 5", 0)
        , TestTemporal(6, "New Value 6", 0)
        , TestTemporal(7, "New Value 7", 0)
        , TestTemporal(8, "Value8", 0)
        , TestTemporal(9, "Value9", 0)
        , TestTemporal(2, "Value2", 1)
        , TestTemporal(5, "Value5", 1)
        , TestTemporal(6, "Value6", 1)
        , TestTemporal(7, "Value7", 1)
      ))

      val maxTS = Timestamp.valueOf(testTemporal.agg(max($"system_timestamp_of_extraction")).as[String].collect().head)

      val snapshotReadFlow =
        flow.snapshotTemporalTablesFromStorage(s"$testingBaseDir/output", maxTS)("testtemporal")

      val snapshotRes = executor.execute(snapshotReadFlow)

      val testTemporalSnapshot = snapshotRes._2.inputs.get[Dataset[_]]("testtemporal")

      testTemporalSnapshot.sort("testtemporalid")
        .as[TestTemporal].collect() should be(Seq(
        TestTemporal(1, "New Value 1", 0)
        , TestTemporal(3, "Value3", 0)
        , TestTemporal(4, "Value4", 0)
        , TestTemporal(5, "New Value 5", 0)
        , TestTemporal(6, "New Value 6", 0)
        , TestTemporal(7, "New Value 7", 0)
        , TestTemporal(8, "Value8", 0)
        , TestTemporal(9, "Value9", 0)
      ))

    }
  }
}

object ActionInterpreter extends SQLServerTemporalExtractorBase {

  import SQLServerTemporalExtractorBase._
  import monix.execution.Scheduler.Implicits.global
  import scalikejdbc._
  import cats.data.OptionT
  import cats.implicits._

  Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
  ConnectionPool.singleton(sqlServerConnectionDetails.jdbcString, sqlServerConnectionDetails.user, sqlServerConnectionDetails.password)

  implicit def TaskTxBoundary[A]: TxBoundary[Task[A]] = new TxBoundary[Task[A]] {
    override def finishTx(result: Task[A], tx: Tx): Task[A] =
      result.attempt.flatMap {
        case Right(_) => Task(tx.commit()).flatMap(_ => result)
        case Left(_) => Task(tx.rollback()).flatMap(_ => result)
      }

    override def closeConnection(result: Task[A], doClose: () => Unit): Task[A] = {
      for {
        x <- result
        _ <- Task(doClose).map(_.apply())
      } yield x
    }
  }

  def runActions(actions: Seq[Action[Row]]) = {
    Task.parSequenceN(1)(actions.map {
      case Add(row) => addRow(row)
    })
  }

  private def addRow(row: TestTemporal): Task[AppliedAction] = {
    val insertValue = row.testtemporalvalue
    DB.localTx(implicit session => {
      val ins: OptionT[Task, Inserted] = OptionT.fromOption[Task](
        sql"""insert into TestTemporal (TestTemporalValue)
              output inserted.TestTemporalID as id, inserted.SysEndTime as time
              values ($insertValue)"""
          .map(rs => Inserted(rs.string("id").toInt, Timestamp.valueOf(rs.string("time")).toInstant))
          .single().apply()
      )

      (for {
        row <- ins
        appliedAction <- OptionT.pure[Task](AppliedAction(
          Add(TestTemporal(row.id, insertValue, 0)),
          row.timestamp
        ))
      } yield appliedAction).value.map(_.get)
    }
    )(boundary = TaskTxBoundary)
  }

  case class Inserted(id: Int, timestamp: Instant)

}