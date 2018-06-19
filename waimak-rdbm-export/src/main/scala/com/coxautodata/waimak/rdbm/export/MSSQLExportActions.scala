package com.coxautodata.waimak.rdbm.export

import com.coxautodata.waimak.dataflow.{ActionResult, DataFlowException}
import com.coxautodata.waimak.dataflow.spark.{SimpleAction, SparkDataFlow}
import com.coxautodata.waimak.log.Logging
import org.apache.spark.sql.{Dataset, SaveMode}

/**
  * Created by Alexei Perelighin
  */
object MSSQLExportActions {

  /**
    * Defines implicits for pushing data from staging into MS SQL RDBM
    *
    * @param sparkDataFlow
    */
  implicit class SparkMSSQL(sparkDataFlow: SparkDataFlow) extends Logging {

    type returnType = ActionResult[Dataset[_]]

    /**
      * Push staged label into rdbm table, which must already exist.
      *
      * @param flow
      * @param jdbc
      * @param label
      * @param table
      * @return
      */
    protected def pushToMSSQL(flow: SparkDataFlow, jdbc: String, label: String, table: String): SparkDataFlow = {

      def run(m: Map[String, Dataset[_]]): returnType = {
        logInfo(s"Preparing to push data from [${label}] into table [${table}]")
        val connectionProperties = new java.util.Properties()
        connectionProperties.setProperty("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        m(label).write.mode(SaveMode.Append).jdbc(jdbc, table, connectionProperties)
        logInfo(s"Finished pushing data from [${label}] into table [${table}]")
        Seq.empty
      }

      flow.addAction(new SimpleAction(List(label), List.empty, run))
    }

    /**
      * Push staged label into rdbm table, which must already exist.
      *
      * @param jdbc
      * @param label
      * @param table
      * @return
      */
    def pushToMSSQL(jdbc: String, label: String, table: String): SparkDataFlow = {
      pushToMSSQL(sparkDataFlow, jdbc, label, table)
    }

    /**
      * Push into rdbm multiple labels into tables with same names as labels.
      *
      * @param jdbc
      * @param schema
      * @param tableLabels
      * @return
      */
    def pushToMSSQL(jdbc: String, schema: String, tableLabels: String*): SparkDataFlow = {
      if (tableLabels.isEmpty) throw new DataFlowException("Push to MS. Label list is empty.")
      tableLabels.foldLeft(sparkDataFlow)((res, label) => pushToMSSQL(res, jdbc, label, s"[${schema}].[${label}]"))
    }
  }

}
