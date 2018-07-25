package com.coxautodata.waimak.azure.table

import java.sql.{Date, Timestamp}

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
    * @return A map of column name to function that transforms a row into the Azure Table datatype
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
          case unknownType => throw new RuntimeException("Unknown type conversion for Azure Table: " + unknownType.typeName)
        }
        (field.name, encoder)
      }.toMap
  }

  def getTypeOrElseNull[T](row: Row, pos: Int, getter: (Row, Int) => T = (r: Row, p: Int) => r.getAs[T](p)): T = {
    if (row.isNullAt(pos)) null.asInstanceOf[T] else getter(row, pos)
  }

  // Have to use java lang boxed primitives to force the null to be preserved as scala will convert its types to primitive java types

  def boolean(row: Row, pos: Int): EntityProperty = new EntityProperty(getTypeOrElseNull[java.lang.Boolean](row, pos))

  def byte(row: Row, pos: Int): EntityProperty = new EntityProperty(getTypeOrElseNull[java.lang.Integer](row, pos, (r, p) => r.getByte(p).toInt))

  def date(row: Row, pos: Int): EntityProperty = new EntityProperty(getTypeOrElseNull[Date](row, pos, (r, p) => r.getDate(p)))

  def decimal(row: Row, pos: Int): EntityProperty = new EntityProperty(getTypeOrElseNull[java.lang.Double](row, pos, (r, p) => r.getDecimal(p).doubleValue()))

  def double(row: Row, pos: Int): EntityProperty = new EntityProperty(getTypeOrElseNull[java.lang.Double](row, pos))

  def float(row: Row, pos: Int): EntityProperty = new EntityProperty(getTypeOrElseNull[java.lang.Double](row, pos, (r, p) => r.getFloat(p).doubleValue()))

  def string(row: Row, pos: Int): EntityProperty = new EntityProperty(getTypeOrElseNull[java.lang.String](row, pos))

  def int(row: Row, pos: Int): EntityProperty = new EntityProperty(getTypeOrElseNull[java.lang.Integer](row, pos))

  def long(row: Row, pos: Int): EntityProperty = new EntityProperty(getTypeOrElseNull[java.lang.Long](row, pos))

  def short(row: Row, pos: Int): EntityProperty = new EntityProperty(getTypeOrElseNull[java.lang.Integer](row, pos, (r, p) => r.getShort(p).toInt))

  def timestamp(row: Row, pos: Int): EntityProperty = new EntityProperty(getTypeOrElseNull[Timestamp](row, pos, (r, p) => r.getTimestamp(p)))

}
