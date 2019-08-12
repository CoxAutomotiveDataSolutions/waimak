package com.coxautodata.waimak.configuration

import java.io.InputStreamReader
import java.util.Properties

import com.coxautodata.waimak.configuration.CaseClassConfigParser.CONFIG_PROPERTIES_FILE_URI
import com.coxautodata.waimak.dataflow.spark.SparkFlowContext
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

/**
  * A [[PropertyProviderBuilder]] object that reads property key-values from
  * a Java Properties file.
  * Set the properties file URI by setting the [[CONFIG_PROPERTIES_FILE_URI]] Spark
  * configuration value.
  * File will be opened using an Hadoop FileSystem object, therefore URI must be supported by your
  * Hadoop libraries and configuration must be present in the HadoopConfiguration on the SparkSession.
  */
object PropertiesFilePropertyProviderBuilder extends PropertyProviderBuilder {

  val charsetName: String = "UTF-8"

  private def getPropertiesFile(file: String, spark: SparkSession): Properties = {
    val path = new Path(file)
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val config: Properties = new Properties()
    val stream = fs.open(path)
    val stream2 = new InputStreamReader(stream, charsetName)
    config.load(stream2)
    stream2.close()
    stream.close()
    config
  }

  override def getPropertyProvider(context: SparkFlowContext): PropertyProvider = {
    val filename = context
      .getOption(CONFIG_PROPERTIES_FILE_URI)
      .getOrElse(throw new NoSuchElementException(s"Could not find value for property: $CONFIG_PROPERTIES_FILE_URI"))
    val props = getPropertiesFile(filename, context.spark)
    new JavaPropertiesPropertyProvider(props)
  }
}
