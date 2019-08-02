package com.coxautodata.waimak.dataflow.spark.dataquality

import com.coxautodata.waimak.configuration.CaseClassConfigParser
import com.coxautodata.waimak.dataflow.spark.SparkFlowContext
import com.coxautodata.waimak.dataflow.spark.dataquality.AlertImportance.{Critical, Good, Information, Warning}
import com.coxautodata.waimak.dataflow.spark.dataquality.DataQualityConfigurationExtension.DATAQUALITY_ALERTERS
import io.circe
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.{PostMethod, StringRequestEntity}
import org.apache.http.client.HttpResponseException

import scala.util.Try

case class SlackQualityAlert(token: String, alertOn: List[AlertImportance] = List.empty) extends DataQualityAlertHandler {

  private def toJson(alert: DataQualityAlert): String = {
    val slackColour = alert.importance match {
      case Critical => SlackDanger
      case Warning => SlackWarning
      case Good => SlackGood
      case Information => SlackInformation
    }
    SlackMessage(attachments = Some(Seq(SlackAttachment(Some(alert.alertMessage), color = Some(slackColour)))))
      .asJson
      .noSpaces
  }

  override def handleAlert(alert: DataQualityAlert): Try[Unit] = Try {
    val json = toJson(alert)
    val post = new PostMethod(s"https://hooks.slack.com/services/$token")
    post.setRequestHeader("Content-type", "application/json")
    post.setRequestEntity(new StringRequestEntity(json, "application/json", "UTF-8"))
    val response = new HttpClient().executeMethod(post)
    val responseStatus = response
    if (responseStatus != 200) {
      throw new HttpResponseException(responseStatus, s"Invalid response status, got $responseStatus")
    }
  }
}

class SlackQualityAlertService extends DataQualityAlertHandlerService {
  override def handlerKey: String = "slack"

  override def getAlertHandler(flowContext: SparkFlowContext): DataQualityAlertHandler = {
    val conf = CaseClassConfigParser[SlackQualityAlertConfig](flowContext, s"${DATAQUALITY_ALERTERS}.slack.")
    SlackQualityAlert(conf.token, conf.alertOnImportances)
  }
}

private[dataquality] case class SlackQualityAlertConfig(token: String, alertOn: List[String]) {
  def alertOnImportances: List[AlertImportance] = alertOn.map(AlertImportance(_))
}

sealed abstract class SlackColor(val value: String)

case object SlackDanger extends SlackColor("danger")

case object SlackWarning extends SlackColor("warning")

case object SlackGood extends SlackColor("good")

case object SlackInformation extends SlackColor("#439FE0")

object SlackColor {
  implicit val encodeSlackColor: io.circe.Encoder[SlackColor] = new circe.Encoder[SlackColor] {
    override def apply(a: SlackColor): Json = a.value.asJson
  }
}

case class SlackField(title: String, value: String)

case class SlackAttachment(title: Option[String] = None, title_link: Option[String] = None, color: Option[SlackColor] = None,
                           ts: Option[String] = None, footer: Option[String] = None, fields: Option[Seq[SlackField]] = None)

case class SlackMessage(text: Option[String] = None, attachments: Option[Seq[SlackAttachment]] = None)
