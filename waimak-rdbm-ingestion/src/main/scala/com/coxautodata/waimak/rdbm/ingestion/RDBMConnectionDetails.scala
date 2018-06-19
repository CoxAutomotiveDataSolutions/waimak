package com.coxautodata.waimak.rdbm.ingestion

/**
  * Created by Vicky Avison on 30/04/18.
  */
trait RDBMConnectionDetails {

  def jdbcString: String

  def user: String

  def password: String
}
