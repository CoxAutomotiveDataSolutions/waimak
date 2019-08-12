package com.coxautodata.waimak.dataflow.spark.dataquality

import java.time.Instant
import java.util.{Date, Properties}

import com.coxautodata.waimak.configuration.CaseClassConfigParser
import com.coxautodata.waimak.dataflow.DataFlowException
import com.coxautodata.waimak.dataflow.spark.SparkFlowContext
import com.coxautodata.waimak.dataflow.spark.dataquality.DataQualityConfigurationExtension.DATAQUALITY_ALERTERS
import javax.mail.Message.RecipientType._
import javax.mail._
import javax.mail.internet.{InternetAddress, MimeMessage}

import scala.util.Try

/**
  * Sends alerts via email
  *
  * @param settings the email settings to use
  * @param alertOn  If specified, the list of alert importance levels to alert on. If unspecified or empty, every level
  *                 will be alerted on.
  */
case class EmailQualityAlert(settings: EmailSettings, alertOn: List[AlertImportance] = List.empty) extends BaseEmailQualityAlert {
  override def provider: Option[Provider] = None

  override def defaultProperties: Properties = new Properties()
}

class EmailQualityAlertService extends DataQualityAlertHandlerService {
  override def handlerKey: String = "email"

  override def getAlertHandler(flowContext: SparkFlowContext): DataQualityAlertHandler = {
    val importanceConf = CaseClassConfigParser[EmailAlertImportance](flowContext, s"$DATAQUALITY_ALERTERS.email.")
    val emailConf = CaseClassConfigParser[SMTPEmailSettings](flowContext, s"$DATAQUALITY_ALERTERS.email.")
    EmailQualityAlert(emailConf, importanceConf.alertOnImportances)
  }
}

private[dataquality] case class EmailAlertImportance(alertOn: List[String]) {
  def alertOnImportances: List[AlertImportance] = alertOn.map(AlertImportance(_))
}

trait BaseEmailQualityAlert extends DataQualityAlertHandler {

  def provider: Option[Provider]

  def defaultProperties: Properties

  def settings: EmailSettings

  def handleAlert(alert: DataQualityAlert): Try[Unit] = Try {
    val message: Message = settings.getMessage(defaultProperties, provider)
    message.setSentDate(Date.from(Instant.now()))
    message.setSubject(s"Data Quality Alert: ${alert.importance.description}")
    message.setText(alert.alertMessage)
    Transport.send(message)
  }
}

/**
  * Email settings used to configure an [[EmailQualityAlert]].
  * Used to build a specific implementation of a settings object, such
  * as [[SMTPEmailSettings]]
  */
trait EmailSettings {
  def getMessage(defaultProperties: Properties, provider: Option[Provider]): Message
}

/**
  * Email settings used to configure an [[EmailQualityAlert]] for SMTP emails
  *
  * @param to                (Optional) comma-separated list of 'to' destination addresses
  * @param cc                (Optional) comma-separated list of 'cc' destination addresses
  * @param bcc               (Optional) comma-separated list of 'bcc' destination addresses
  * @param from              (Optional) from address in email message
  * @param host              (Mandatory) hostname/address of email server
  * @param port              (Optional) port of email server, default 25
  * @param auth              (Optional) whether to use authentication to email server, default false
  * @param starttls          (Optional) whether to enable starttls when communicating with email server, default true
  * @param ssl               (Optional) whether to force connections only over SSL, default false
  * @param user              (Optional) username to use if authentication enabled
  * @param pass              (Optional) password to use if authentication enabled
  * @param connectionTimeout (Optional) Socket connection timeout in MS, default 1000ms
  * @param timeout           (Optional) Socket I/O connection timeout in MS, default 1000ms
  * @param debug             (Optional) Whether to enable debugging on the session object, default true
  */
case class SMTPEmailSettings(to: List[String] = List.empty,
                             cc: List[String] = List.empty,
                             bcc: List[String] = List.empty,
                             from: Option[String] = None,
                             host: String,
                             port: Int = 25,
                             auth: Boolean = false,
                             starttls: Boolean = true,
                             ssl: Boolean = false,
                             user: Option[String] = None,
                             pass: Option[String] = None,
                             connectionTimeout: Long = 1000,
                             timeout: Long = 1000,
                             debug: Boolean = false) extends EmailSettings {
  def getMessage(defaultProperties: Properties, provider: Option[Provider]): Message = {
    val properties = defaultProperties
    properties.setProperty("mail.transport.protocol", "smtp")
    properties.setProperty("mail.smtp.connectiontimeout", connectionTimeout.toString)
    properties.setProperty("mail.smtp.timeout", timeout.toString)
    properties.setProperty("mail.smtp.host", host)
    properties.setProperty("mail.smtp.port", port.toString)
    properties.setProperty("mail.smtp.auth", auth.toString)
    properties.setProperty("mail.smtp.starttls.enable", starttls.toString)
    properties.setProperty("mail.smtp.ssl.enable", ssl.toString)

    val authenticator = (auth, user, pass) match {
      case (false, _, _) => None
      case (true, None, _) | (true, _, None) => throw new DataFlowException("Both user and pass must be specified when using auth=true")
      case (true, Some(u), Some(p)) => Some {
        new Authenticator() {
          override protected def getPasswordAuthentication: PasswordAuthentication = {
            new PasswordAuthentication(u, p)
          }
        }
      }
    }
    val session = Session.getDefaultInstance(properties, authenticator.orNull)
    session.setDebug(debug)
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