package waimak.azure.table

import com.microsoft.azure.storage.table.EntityProperty
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
  * Creates a converter from spark Row into Azure EntityProperty.
  *
  * Created by alexeipab on 18/12/17.
  */
object PropertyEncoder {

  type encoderType = Row => EntityProperty

  /**
    * Encoder converts Dataset StructTypes into Azure Table entities (objects from the azure table APIs).
    * These objects can then be pushed into an Azure Table.
    * It also does the necessary data type conversions.
    * E.g., decimal(38, 2) does not exist as a type in Azure Tables therefore it is converted into a Double.
    *
    * @param schema       Source schema to convert
    * @param ignoreFields Fields to ignore from the source schema
    * @return             A map of column name to function that transforms a row into the Azure Table datatype
    */
  def toEncoder(schema: StructType, ignoreFields: Set[String]): Map[String, encoderType] = {
    schema.fields.zipWithIndex.filter(f => !ignoreFields.contains(f._1.name))
      .map { fd =>
        val field = fd._1
        val pos = fd._2

        val encoder: encoderType = field.dataType match {
          case BooleanType => (row: Row) => boolean(row, pos)
          case ByteType => (row: Row) => byte(row, pos)
          case DateType => (row: Row) => date(row, pos)
          case _: DecimalType => (row: Row) => decimal(row, pos)
          case DoubleType => (row: Row) => double(row, pos)
          case FloatType => (row: Row) => float(row, pos)
          case StringType => (row: Row) => string(row, pos)
          case IntegerType => (row: Row) => int(row, pos)
          case LongType => (row: Row) => long(row, pos)
          case ShortType => (row: Row) => short(row, pos)
          case TimestampType => (row: Row) => timestamp(row, pos)
          case unknownType => throw new RuntimeException("Unknown type conversion for Azure Table" + unknownType.typeName)
        }
        (field.name, encoder)
      }.toMap
  }

  def boolean(row: Row, pos: Int): EntityProperty = new EntityProperty(row.getAs[Boolean](pos))

  def byte(row: Row, pos: Int): EntityProperty = new EntityProperty(row.getAs[Byte](pos))

  def date(row: Row, pos: Int): EntityProperty = new EntityProperty(row.getDate(pos))

  def decimal(row: Row, pos: Int): EntityProperty = {
    val dc = row.getDecimal(pos)
    val db: Double = if (dc != null) dc.doubleValue() else null.asInstanceOf[Double]
    new EntityProperty(db)
  }

  def double(row: Row, pos: Int): EntityProperty = new EntityProperty(row.getAs[Double](pos))

  def float(row: Row, pos: Int): EntityProperty = new EntityProperty(row.getAs[Float](pos))

  def string(row: Row, pos: Int): EntityProperty = new EntityProperty(row.getAs[String](pos))

  def int(row: Row, pos: Int): EntityProperty = new EntityProperty(row.getAs[Int](pos))

  def long(row: Row, pos: Int): EntityProperty = new EntityProperty(row.getAs[Long](pos))

  def short(row: Row, pos: Int): EntityProperty = new EntityProperty(row.getAs[Short](pos))

  def timestamp(row: Row, pos: Int): EntityProperty = new EntityProperty(row.getTimestamp(pos))

}
