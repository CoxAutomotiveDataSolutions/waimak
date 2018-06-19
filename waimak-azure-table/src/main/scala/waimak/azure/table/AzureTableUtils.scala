package waimak.azure.table

import java.util

import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.table.{DynamicTableEntity, EntityProperty}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Column, Dataset, Row}

/**
  * Created by Alexei Perelighin on 18/12/17.
  */
object AzureTableUtils {

  val datasetPartitionColumn = "_partition"

  val datasetIDColumn = "_id"

  val metaColumns = Set(datasetIDColumn, datasetPartitionColumn)

  /**
    * Creates a `_partition` column in the Dataset from an existing column cast as StringType.
    * This column will be used for inserting into Azure Table partitions (`PartitionKey`).
    */
  def addAzurePartition(ds: Dataset[_], column: Column): Dataset[Row] = ds.withColumn(datasetPartitionColumn, column.cast(StringType))

  /**
    * Creates an `_id` column in the Dataset from an existing column cast as StringType.
    * This column is used as the `RowKey` for a given row.
    */
  def addAzureID(ds: Dataset[_], column: Column): Dataset[Row] = ds.withColumn(datasetIDColumn, column.cast(StringType))

  /**
    * Creates both the `_partition` and `_id` columns described above.
    */
  def addAzureMeta(ds: Dataset[_], partition: Column, id: Column): Dataset[Row] = addAzurePartition(addAzureID(ds, id), partition)

}
