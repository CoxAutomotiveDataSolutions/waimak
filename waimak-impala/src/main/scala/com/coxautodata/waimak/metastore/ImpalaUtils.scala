package com.coxautodata.waimak.metastore

import java.sql.Timestamp

import com.coxautodata.waimak.log.Logging
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{DateType, TimestampType}

/**
  * Created by Alexei Perelighin on 18/07/17.
  */
object ImpalaUtils extends Logging {

  val timezone = "UTC"

  /**
    * Impala has limitation on the range of dates that it can accept and all dates must be in UTC
    */
  val (impalaTimestampLow, impalaTimestampHigh) = {
    val parser = new java.text.SimpleDateFormat("yyyy-MM-dd")
    parser.setTimeZone(java.util.TimeZone.getTimeZone(timezone))
    (parser.parse("1400-01-01"), parser.parse("9999-12-31"))
  }


  val lowTimestamp = new Timestamp(impalaTimestampLow.getTime)

  /**
    * Impala does not support dates before 1400, so if the date is not NULL and is outside valid range, it is defaulted to 1400.
    */
  def updateTimestampToImpala: UserDefinedFunction = udf((ts: Timestamp) =>
    Option(ts).map(d => if (d.after(impalaTimestampLow) && d.before(impalaTimestampHigh)) d else lowTimestamp)
  )

  /**
    * Lower cases all column names and casts all Date types to TimestampType
    *
    * @param df
    * @return
    */
  def amendDataTypesForImpala(df: DataFrame): DataFrame = {
    val select = df.schema.map { sf =>
      val column = sf.dataType match {
        case DateType => updateTimestampToImpala(df(sf.name).cast(TimestampType)).as(sf.name)
        case TimestampType => updateTimestampToImpala(df(sf.name)).as(sf.name)
        case _ => df(sf.name)
      }
      column.as(standardizeName(sf.name))
    }
    df.select(select: _*)
  }

  def standardizeName(name: String): String = StringUtils.replaceEach(name.trim.toLowerCase, Array("'", " ", "-", "\\", "/", ".", "#", "&", "%"), Array("", "_", "_", "_", "_", "_", "_", "_", "_perc"))

}
