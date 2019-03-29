package com.coxautodata.waimak.dataflow

trait FlowContext {

  /** Get a configuration value, returning a None if not set */
  def getOption(key: String): Option[String]

  /** Get a configuration value as a string, falling back to a default if not set */
  def getString(key: String, defaultValue: String): String = {
    getOption(key).getOrElse(defaultValue)
  }

  /** Get a configuration value as an integer, falling back to a default if not set */
  def getInt(key: String, defaultValue: Int): Int = {
    getOption(key).map(_.toInt).getOrElse(defaultValue)
  }

  /** Get a configuration value as a long, falling back to a default if not set */
  def getLong(key: String, defaultValue: Long): Long = {
    getOption(key).map(_.toLong).getOrElse(defaultValue)
  }

  /** Get a configuration value as a double, falling back to a default if not set */
  def getDouble(key: String, defaultValue: Double): Double = {
    getOption(key).map(_.toDouble).getOrElse(defaultValue)
  }

  /** Get a configuration value as a boolean, falling back to a default if not set */
  def getBoolean(key: String, defaultValue: Boolean): Boolean = {
    getOption(key).map(_.toBoolean).getOrElse(defaultValue)
  }

  /** Get a configuration value as a list of strings with "," separator, falling back to a default if not set */
  def getStringList(key: String, defaultValue: List[String]): List[String] = {
    getOption(key).map(_.split(',').toList).getOrElse(defaultValue)
  }

  def setPoolIntoContext(poolName: String): Unit

  def reportActionStarted(action: DataFlowAction): Unit

  def reportActionFinished(action: DataFlowAction): Unit

}
