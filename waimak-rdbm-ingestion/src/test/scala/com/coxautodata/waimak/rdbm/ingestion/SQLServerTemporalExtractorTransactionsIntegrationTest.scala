package com.coxautodata.waimak.rdbm.ingestion

import java.sql.Timestamp
import java.time.Instant

import com.coxautodata.waimak.dataflow.Waimak
import com.coxautodata.waimak.dataflow.spark.SparkAndTmpDirSpec
import com.coxautodata.waimak.rdbm.ingestion.RDBMIngestionActions._
import com.coxautodata.waimak.storage.AuditTableInfo
import com.coxautodata.waimak.storage.StorageActions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.max
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.collection.mutable.MutableList

/**
 * Test that the temporal extractor works correctly in the face of transactions
 *
 * Created by James Fielder on 2020/06/09
 */
class SQLServerTemporalExtractorTransactionsIntegrationTest
  extends SparkAndTmpDirSpec
    with BeforeAndAfterEach
    with SQLServerTemporalExtractorBase {
  import SQLServerTemporalExtractorBase._

  override def beforeEach(): Unit = {
    super.beforeEach()
    cleanupTables()
    setupTables()
  }

  override def afterEach(): Unit = {
    super.afterEach()
//    cleanupTables()
  }

  override val appName: String = "SQLServerTemporalConnectorTransactionsIntegrationTest"

  describe("SQLServerTemporalExtractor"){
    it("should do basic inserts and deletes with DSL") {
      val actions = MutableList[AppliedAction]()

      val inserts = Seq(Add(TestTemporal(1, "test", 0)))
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

  def runActions(actions: Seq[Action[DBData]]) = {
    actions.foreach {
      case Add(row) => ???
    }
  }
}