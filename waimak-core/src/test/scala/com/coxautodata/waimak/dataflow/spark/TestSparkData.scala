package com.coxautodata.waimak.dataflow.spark

import java.sql.Timestamp
import java.text.SimpleDateFormat

/**
  * Created by Alexei Perelighin on 2018/02/28
  */
object TestSparkData {

  val basePath = getClass.getResource("/waimak/simpleaction/read/").getPath

  val purchases = Seq(
    TPurchase(Some(1), Some(1), Some(2))
    , TPurchase(Some(1), Some(1), Some(4))
    , TPurchase(Some(1), Some(2), Some(1))
    , TPurchase(Some(2), Some(2), Some(2))
    , TPurchase(Some(3), Some(1), Some(2))
    , TPurchase(Some(5), Some(1), Some(1))
    , TPurchase(Some(5), Some(2), Some(1))
    , TPurchase(Some(5), Some(3), Some(1))
  )

  val persons = Seq(
    TPerson(Some(1), Some("John"), Some("UK"))
    , TPerson(Some(2), Some("Paul"), Some("UK"))
    , TPerson(Some(3), Some("George"), Some("UK"))
    , TPerson(Some(4), Some("Ringo"), Some("UK"))
    , TPerson(Some(5), Some("Yoko"), Some("JP"))
  )

  val report = Seq(
    TReport(Some(1), Some("John"), Some("UK"), Some(3), Some(7), Some(2))
    , TReport(Some(2), Some("Paul"), Some("UK"), Some(1), Some(2), Some(2))
    , TReport(Some(3), Some("George"), Some("UK"), Some(1), Some(2), Some(2))
    , TReport(Some(4), Some("Ringo"), Some("UK"), None, None, Some(2))
    , TReport(Some(5), Some("Yoko"), Some("JP"), Some(3), Some(3), Some(2))
  )

  val persons_2 = Seq(
    TPerson(Some(6), Some("Robert"), Some("UK"))
    , TPerson(Some(2), Some("Jimmy"), Some("UK"))
    , TPerson(Some(3), Some("John"), Some("UK"))
  )

  val persons_3 = Seq(
    TPerson(Some(7), Some("Roger"), Some("UK"))
    , TPerson(Some(8), Some("David"), Some("UK"))
    , TPerson(Some(9), Some("Syd"), Some("UK"))
  )

  val persons_4 = Seq(
    TPerson(Some(7), Some("Niel"), Some("US"))
  )

  val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm")
  val lastTS_1 = new Timestamp(formatter.parse("2018-01-01 00:00").getTime)
  val lastTS_2 = new Timestamp(formatter.parse("2018-01-02 00:00").getTime)
  val lastTS_3 = new Timestamp(formatter.parse("2018-01-03 00:00").getTime)
  val lastTS_4 = new Timestamp(formatter.parse("2018-01-04 00:00").getTime)
  val lastTS_5 = new Timestamp(formatter.parse("2018-01-05 00:00").getTime)
  val lastTS_6 = new Timestamp(formatter.parse("2018-01-06 00:00").getTime)
  val lastTS_7 = new Timestamp(formatter.parse("2018-01-07 00:00").getTime)


  val persons_evolved_compacted_1 = Seq(
    TPersonEvolved(Some(1), Some("John"), Some("UK"), Some("2018-01-01"), Some(lastTS_1), None)
    , TPersonEvolved(Some(2), Some("Paul"), Some("UK"), Some("2018-01-01"), Some(lastTS_1), None)
    , TPersonEvolved(Some(3), Some("George"), Some("UK"), Some("2018-01-01"), Some(lastTS_1), None)
    , TPersonEvolved(Some(4), Some("Ringo"), Some("UK"), Some("2018-01-01"), Some(lastTS_1), None)
    , TPersonEvolved(Some(5), Some("Yoko"), Some("JP"), Some("2018-01-01"), Some(lastTS_1), None)
    , TPersonEvolved(Some(6), Some("Robert"), Some("UK"), Some("2018-01-02"), Some(lastTS_2), Some(9))
    , TPersonEvolved(Some(2), Some("Jimmy"), Some("UK"), Some("2018-01-02"), Some(lastTS_2), Some(9))
    , TPersonEvolved(Some(3), Some("John"), Some("UK"), Some("2018-01-02"), Some(lastTS_2), Some(9))
  )
}

case class TPurchase(id: Option[Int], item: Option[Int], amount: Option[Int])

case class TPerson(id: Option[Int], name: Option[String], country: Option[String])

case class TSummary(id: Option[Int], item_cnt: Option[Long], total: Option[Long])

case class TReport(id: Option[Int], name: Option[String], country: Option[String], item_cnt: Option[Long], total: Option[Long], calc_1: Option[Int])

case class TPersonEvolved(id: Option[Int], name: Option[String], country: Option[String], lastTS: Option[String]
                          , _de_last_updated: Option[Timestamp], schema_evolution: Option[Int])