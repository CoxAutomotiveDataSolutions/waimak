package waimak.azure.table

import java.util
import java.util.concurrent.TimeUnit

import com.coxautodata.waimak.dataflow.spark.SparkActions._
import com.coxautodata.waimak.dataflow.spark.{SimpleAction, SparkDataFlow}
import com.coxautodata.waimak.dataflow.{ActionResult, DataFlowEntities, DataFlowException}
import com.coxautodata.waimak.log.Logging
import com.microsoft.azure.storage.table.{DynamicTableEntity, EntityProperty}
import com.microsoft.azure.storage.{CloudStorageAccount, StorageException}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType
import waimak.azure.table.SparkAzureTable.{azureWriterOutputLabel, createIfNotExists, pushToTable}

/**
  * Created by Alexei Perelighin on 2018/03/25.
  */
object SparkAzureTable extends Logging {

  def azureWriterOutputLabel(label: String) = s"__azure_writer_$label"

  /**
    * Creates azure table if it does not exist.
    *
    * @param connectionSTR connection string to Azure storage account
    * @param tableName     name of the azure table
    * @return true if table was created, false if it exists
    * @throws StorageException : if table can not be created or other
    */
  def createIfNotExists(connectionSTR: String, tableName: String): Boolean = {
    val storageAccount = CloudStorageAccount.parse(connectionSTR)
    val tableClient = storageAccount.createCloudTableClient
    val cloudTable = tableClient.getTableReference(tableName)
    val res: Boolean = cloudTable.createIfNotExists()
    logInfo(s"Azure Table [$tableName] was created $res")
    res
  }

  /**
    * Pushes dataset records into Azure Table, it will spawn multiple parallel push threads from each executor. Each
    * push thread will try to use Azure Table batch operations with insertOrReplace, for best performance the inputDS
    * needs to be already partitioned by '_partition' columns and sorted within partition by '_partition' and '_id'
    * columns.
    *
    * @param inputDS       input dataset
    * @param connectionSTR connection string to Azure storage account
    * @param tableName     name of the azure table to push into
    * @param threadNum     number of threads to push with from each executor. Default is 50.
    * @param timeoutMs     timeout in milliseconds for an Azure Table operation
    * @param retryDelayMs  delay in milliseconds to wait between successive retries of an Azure Table operation
    *                      in case of failure or timeout. Number of elements in the sequence indicates how number of
    *                      retries for the operation.
    */
  def pushToTable(inputDS: Dataset[_], connectionSTR: String, tableName: String, threadNum: Int,
                  timeoutMs: Long, retryDelayMs: Seq[Long]): Unit = {

    val notSerializable = Set("_partition", "_id")

    import inputDS.sparkSession.implicits._

    val df = inputDS.withColumn("_partition", $"_partition".cast("string")).withColumn("_id", $"_id".cast("string"))

    val dfSchema: StructType = df.schema

    val res: Dataset[Int] = df.mapPartitions { it =>

      val encoders = PropertyEncoder.toEncoder(dfSchema, notSerializable)
      val rowIT = it.map { row =>
        val partition = row.getAs[String]("_partition")
        val id = row.getAs[String]("_id")

        val vals = encoders.foldLeft(new util.HashMap[String, EntityProperty]()) { (res, field) => res.put(field._1, field._2(row)); res }
        val dynamicEntity = new DynamicTableEntity(partition, id, vals)
        dynamicEntity
      }
      val sit = new SameElemIterator[DynamicTableEntity](rowIT.buffered, 100, (a: DynamicTableEntity, b: DynamicTableEntity) => {
        a.getPartitionKey == b.getPartitionKey
      })

      val writer = new AzureTableMultiWriter(tableName, connectionSTR, threadNum, timeoutMs, retryDelayMs)
      writer.run()
      sit.foreach {
        insertBatch: Seq[DynamicTableEntity] =>
          while (!writer.queue.offer(insertBatch, 500, TimeUnit.MILLISECONDS)) {
            if (writer.threadFailed.get()) throw new DataFlowException(s"Thread failure when writing to the Azure Table")
          }

      }

      val total = writer.finish
      Seq(total).iterator
    }
    val total = res.collect().sum
    logInfo(s"TOTAL RECORDS PUSHED INTO AZURE TABLE [$tableName]: $total")
  }
}

object SparkAzureTableActions {

  implicit class SparkAzureTables(sparkDataFlow: SparkDataFlow) extends Logging {

    type returnType = ActionResult

    val mandatoryFields = Set(AzureTableUtils.datasetPartitionColumn, AzureTableUtils.datasetIDColumn)

    protected def validateColumns(label: String, ds: Dataset[_]): Unit = {
      if (mandatoryFields.size != ds.schema.fieldNames.count(mandatoryFields.contains))
        throw new DataFlowException(s"Label $label can not be used for upload to Azure Table, mandatory fields are not present: ${mandatoryFields.mkString("[", ",", "]")}")
    }

    /**
      * Writes all records from the dataset behind a label into an Azure Table.
      *
      * @param connection   connection string to Azure storage account
      * @param tableName    name of the azure table, if different to the label name. If None, then label name will be
      *                     used for azure table name
      * @param threadNum    number of threads to push with from each executor. Default is 50.
      * @param timeoutMs    timeout in milliseconds for an Azure Table operation
      * @param retryDelayMs delay in milliseconds to wait between successive retries of an Azure Table operation
      *                     in case of failure or timeout. Number of elements in the sequence indicates how number of
      *                     retries for the operation.
      * @param label        waimak label
      * @return
      */
    def writeAzureTable(connection: String, tableName: Option[String] = None, threadNum: Int = 50,
                        timeoutMs: Long = 10000, retryDelayMs: Seq[Long] = Seq(1000, 2000, 4000, 8000))(label: String): SparkDataFlow = {

      val table = tableName.getOrElse(label)

      def run(dfs: DataFlowEntities): returnType = {
        val df = dfs.get[Dataset[_]](label)
        import df.sparkSession.implicits._
        logInfo(s"Preparing to push data into table [$table]")
        validateColumns(label, df)
        createIfNotExists(connection, table)
        pushToTable(df, connection, table, threadNum, timeoutMs, retryDelayMs)
        val successDS = sparkDataFlow.flowContext.spark.createDataset(Array(WriterSuccess(1)))
        Seq(Some(successDS))
      }

      sparkDataFlow.addAction(new SimpleAction(List(label), List(azureWriterOutputLabel(label)), run))
    }

    /**
      * Azure Table batch operations allow only 100 records per batch per partition, it is not possible to mix
      * records from different azure table partitions in one batch. This will correctly repartition and sort the
      * dataset for it to be pushed efficiently into Azure Table.
      *
      * The reorganised data set can be saved into storage and than read back for writing to azure table.
      *
      * @param inLabel
      * @param outLabel
      * @return
      */
    def azureRepartition(inLabel: String)(outLabel: String): SparkDataFlow = {
      sparkDataFlow.transform(inLabel)(outLabel) { inDS =>
        validateColumns(inLabel, inDS)
        inDS.repartition(inDS(AzureTableUtils.datasetPartitionColumn))
          .sortWithinPartitions(AzureTableUtils.datasetPartitionColumn, AzureTableUtils.datasetIDColumn)
      }
    }
  }

}

case class WriterSuccess(i: Int)