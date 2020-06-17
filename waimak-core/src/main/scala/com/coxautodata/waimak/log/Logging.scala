package com.coxautodata.waimak.log

import org.slf4j.{Logger, LoggerFactory}
import enumeratum._
import scala.collection.immutable
import scala.util.{Failure, Success, Try}

sealed trait Level extends EnumEntry

object Level extends Enum[Level] {
  override def values: immutable.IndexedSeq[Level] = findValues

  case object Info extends Level
  case object Debug extends Level
  case object Trace extends Level
  case object Warning extends Level
  case object Error extends Level
}

/**
  * Created by Vicky Avison on 05/06/17.
  */
trait Logging {

  import Level._

  // Method to get the logger name for this object
  protected def logName: String = {
    // Ignore trailing $'s in the class names for Scala objects
    this.getClass.getName.stripSuffix("$")
  }

  private val log: Logger = LoggerFactory.getLogger(logName)

  // Log methods that take only a String
  protected def logInfo(msg: => String) {
    if (log.isInfoEnabled) log.info(msg)
  }

  protected def logDebug(msg: => String) {
    if (log.isDebugEnabled) log.debug(msg)
  }

  protected def logTrace(msg: => String) {
    if (log.isTraceEnabled) log.trace(msg)
  }

  protected def logWarning(msg: => String) {
    if (log.isWarnEnabled) log.warn(msg)
  }

  protected def logError(msg: => String) {
    if (log.isErrorEnabled) log.error(msg)
  }

  // Log methods that take Throwables (Exceptions/Errors) too
  protected def logInfo(msg: => String, throwable: Throwable) {
    if (log.isInfoEnabled) log.info(msg, throwable)
  }

  protected def logDebug(msg: => String, throwable: Throwable) {
    if (log.isDebugEnabled) log.debug(msg, throwable)
  }

  protected def logTrace(msg: => String, throwable: Throwable) {
    if (log.isTraceEnabled) log.trace(msg, throwable)
  }

  protected def logWarning(msg: => String, throwable: Throwable) {
    if (log.isWarnEnabled) log.warn(msg, throwable)
  }

  protected def logError(msg: => String, throwable: Throwable) {
    if (log.isErrorEnabled) log.error(msg, throwable)
  }

  protected def isTraceEnabled(): Boolean = {
    log.isTraceEnabled
  }

  /**
   * Takes a value of type A and a function message from A to String, logs the value of
   * invoking message(a) at the level described by the level parameter
   *
   * @example {{{
   *           logAndReturn(1, (num: Int) => s"number: $num", Info)
   *           // In the log we would see a log corresponding to "number 1"
   * }}}
   *
   * @param a
   * @param message
   * @param level
   * @tparam A
   * @return a
   */
  def logAndReturn[A](a: A, message: A => String, level: Level): A =
    Try(message(a)) match {
      case Success(msg) => logAndReturn(a, msg, level)
      case Failure(exception) => logError("logAndReturn message function threw an exception"); exception.printStackTrace(); a
    }

  /**
   * Takes a value of type A and a msg to log, returning a and logging the message at the desired level
   *
   * @param a
   * @param msg
   * @param level
   * @tparam A
   * @return a
   */
  def logAndReturn[A](a: A, msg: String, level: Level): A = {
    level match {
      case Info => logInfo(msg)
      case Debug => logDebug(msg)
      case Trace => logTrace(msg)
      case Warning => logWarning(msg)
      case Error => logError(msg)
    }

    a
  }
}
