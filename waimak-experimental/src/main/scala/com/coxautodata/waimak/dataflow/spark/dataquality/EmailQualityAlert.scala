package com.coxautodata.waimak.dataflow.spark.dataquality

import java.time.Instant
import java.util.{Date, Properties}

import javax.mail.Message.RecipientType._
import javax.mail._
import javax.mail.internet.{InternetAddress, MimeMessage}

case class EmailQualityAlert(settings: EmailSettings, alertOn: List[AlertImportance] = List.empty) extends BaseEmailQualityAlert {
  override def provider: Option[Provider] = None

  override def defaultProperties: Properties = new Properties()
}

trait BaseEmailQualityAlert extends DataQualityAlertHandler {

  def provider: Option[Provider]

  def defaultProperties: Properties

  def settings: EmailSettings

  def handleAlert(alert: DataQualityAlert): Unit = {
    val message: Message = settings.getMessage(defaultProperties, provider)
    message.setSentDate(Date.from(Instant.now()))
    message.setSubject(s"Data Quality Alert: ${alert.importance.description}")
    message.setText(alert.alertMessage)
    Transport.send(message)
  }
}

case class EmailSettings(to: List[String] = List.empty,
                         cc: List[String] = List.empty,
                         bcc: List[String] = List.empty,
                         from: Option[String] = None,
                         host: String,
                         port: Int = 25,
                         auth: Boolean = false,
                         starttls: Boolean = true,
                         user: Option[String] = None,
                         pass: Option[String] = None) {
  def getMessage(defaultProperties: Properties, provider: Option[Provider]): Message = {
    val properties = defaultProperties
    properties.setProperty("mail.smtp.host", host)
    properties.setProperty("mail.smtp.port", port.toString)
    properties.setProperty("mail.smtp.auth", auth.toString)
    properties.setProperty("mail.smtp.starttls.enable", starttls.toString)
    user.foreach(
      properties.setProperty("mail.smtp.user", _)
    )
    pass.foreach(
      properties.setProperty("mail.smtp.pass", _)
    )
    val session = Session.getDefaultInstance(properties)
    provider.foreach(session.setProvider)
    val message = new MimeMessage(session)
    (to.map((_, TO)) ++ cc.map((_, CC)) ++ bcc.map((_, BCC)))
      .foreach {
        case (a, t) => message.addRecipient(t, new InternetAddress(a))
      }
    from.foreach(s => message.setSender(new InternetAddress(s)))
    message
  }
}
