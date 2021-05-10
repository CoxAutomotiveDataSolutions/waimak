package com.coxautodata.waimak.dataflow.spark.dataquality

import java.util.Properties

import com.coxautodata.waimak.dataflow.spark.dataquality.AlertImportance.Warning
import javax.mail.Message.RecipientType._
import javax.mail.Provider
import org.jvnet.mock_javamail.{Mailbox, MockTransport}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class TestEmailQualityAlert extends AnyFunSpec with Matchers {

  val testAlerter: BaseEmailQualityAlert = new BaseEmailQualityAlert {
    override def provider: Option[Provider] = Some(new MockedSMTPProvider)

    override def settings: EmailSettings = SMTPEmailSettings(
      host = "server@host.com",
      to = List("to@host.com"),
      cc = List("cc@host.com"),
      bcc = List("bcc@host.com")
    )

    override def defaultProperties: Properties = new Properties() {
      {
        put("mail.transport.protocol.rfc822", "mocked")
      }
    }

    override def alertOn: List[AlertImportance] = List.empty
  }

  it("should send an alert email") {
    testAlerter.handleAlert(DataQualityAlert("test", Warning))
    val mail = Mailbox.get("to@host.com")
    mail.getNewMessageCount should be(1)
    val message = mail.get(0)
    message.getSubject should be("Data Quality Alert: Warning")
    message.getContent.toString should be("test")
    message.getRecipients(TO).map(_.toString) should contain theSameElementsAs List("to@host.com")
    message.getRecipients(CC).map(_.toString) should contain theSameElementsAs List("cc@host.com")
    message.getRecipients(BCC).map(_.toString) should contain theSameElementsAs List("bcc@host.com")
  }

}

class MockedSMTPProvider extends Provider(Provider.Type.TRANSPORT, "mocked", classOf[MockTransport].getName, "Mock", null)