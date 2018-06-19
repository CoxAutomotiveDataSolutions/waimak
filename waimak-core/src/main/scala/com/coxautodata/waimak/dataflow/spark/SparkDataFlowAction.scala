package com.coxautodata.waimak.dataflow.spark

import com.coxautodata.waimak.dataflow.DataFlowAction
import org.apache.spark.sql.Dataset

trait SparkDataFlowAction extends DataFlowAction[Dataset[_], SparkFlowContext]

