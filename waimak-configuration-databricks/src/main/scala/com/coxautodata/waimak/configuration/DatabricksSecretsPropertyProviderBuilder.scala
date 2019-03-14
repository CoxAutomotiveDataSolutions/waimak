package com.coxautodata.waimak.configuration

import com.coxautodata.waimak.configuration.CaseClassConfigParser.{CONFIG_PROPERTY_PROVIDER_BUILDER_MODULES, configParamPrefix}
import com.coxautodata.waimak.dataflow.spark.SparkFlowContext
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

import scala.util.{Success, Try}

/**
  * A [[PropertyProviderBuilder]] object that reads property key-values from
  * Databricks secret scopes.
  *
  * A comma separated list of Databricks secret scopes to check can be set
  * using [[DatabricksSecretsPropertyProviderBuilder.CONFIG_DATABRICKS_SECRET_SCOPES]].
  * If none are set, all available secret scopes are checked.
  *
  * Include `com.coxautodata.waimak.configuration.DatabricksSecretsPropertyProviderBuilder` in
  * [[CONFIG_PROPERTY_PROVIDER_BUILDER_MODULES]] to use this provider.
  */
object DatabricksSecretsPropertyProviderBuilder extends PropertyProviderBuilder {
  /**
    * A comma separated list of Databricks secret scopes to check when searching for
    * parameter values.
    * If none are set, all available secret scopes are checked.
    * Used when [[CONFIG_PROPERTY_PROVIDER_BUILDER_MODULES]] includes [[DatabricksSecretsPropertyProviderBuilder]].
    */
  val CONFIG_DATABRICKS_SECRET_SCOPES: String = s"$configParamPrefix.databricksSecretScopes"

  override def getPropertyProvider(conf: SparkFlowContext): PropertyProvider = {

    def scopes = conf
      .getOption(CONFIG_DATABRICKS_SECRET_SCOPES)
      .map(_.split(',').toSeq)
      .getOrElse(dbutils.secrets.listScopes().map(_.getName()))
      .toStream

    new PropertyProvider {
      override def get(key: String): Option[String] =
        scopes
          .map(s => Try(dbutils.secrets.get(s, key)))
          .collectFirst { case Success(v) => v }
    }

  }
}
