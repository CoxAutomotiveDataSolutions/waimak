package com.coxautodata.waimak.metastore

import org.apache.derby.client.am.SqlException
import org.scalatest.{BeforeAndAfterAll, WordSpec}

import java.util.Properties
import scala.util.Try

class TestDerby extends WordSpec with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    System.setSecurityManager(null)
    super.beforeAll()
  }

  "derby" should {
    "load" in {
      try Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance
      catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }

    "work" in {
      try Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance
      catch {
        case e: Exception =>
          e.printStackTrace()
      }

      try {
        import java.sql.DriverManager
        val conn = DriverManager.getConnection("jdbc:derby:testdb;create=true", new Properties())
        val st = conn.createStatement
        Try(st.execute("DROP TABLE CUSTOMER"))
        st.executeUpdate("CREATE TABLE  CUSTOMER(ID VARCHAR(255) PRIMARY KEY NOT NULL, NAME VARCHAR(255) NOT NULL, SURNAME VARCHAR(255) NOT NULL)")
        st.executeUpdate("INSERT INTO CUSTOMER VALUES ('1', 'Name1', 'Surname1')")
        st.executeUpdate("INSERT INTO CUSTOMER VALUES ('2', 'Name2', 'Surname2')")
        st.executeUpdate("INSERT INTO CUSTOMER VALUES ('3', 'Name3', 'Surname3')")
        st.executeUpdate("INSERT INTO CUSTOMER VALUES ('4', 'Name4', 'Surname4')")
      } catch {
        case s: SqlException => s.printStackTrace()
      }
    }
  }
}
