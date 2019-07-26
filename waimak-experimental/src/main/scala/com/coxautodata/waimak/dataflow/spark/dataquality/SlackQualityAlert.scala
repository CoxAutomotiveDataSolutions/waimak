package com.coxautodata.waimak.dataflow.spark.dataquality

import io.circe
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.{PostMethod, StringRequestEntity}
import org.apache.http.client.HttpResponseException

case class SlackQualityAlert(token: String) extends DataQualityAlertHandler {

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

  override def handleAlert(alert: DataQualityAlert): Unit = {
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
