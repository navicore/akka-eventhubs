package onextent.akka.eventhubs

import com.microsoft.azure.eventhubs.{EventData, EventHubClient}
import com.typesafe.scalalogging.LazyLogging
import onextent.akka.eventhubs.Connector.AckableOffset

import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object EhWriter extends LazyLogging {

  def apply(eventHubConf: EventHubConf): ((String, String, Option[AckableOffset])) => Future[
    ((String, String, Option[AckableOffset]))] = {

    val ehClient: EventHubClient =
      EventHubClient.createFromConnectionStringSync(eventHubConf.connStr)

    (msg: ((String, String, Option[AckableOffset]))) =>
      val key = msg._1
      val value = msg._2
      logger.debug(s"write key: $key value: $value")
      val payloadBytes = value.getBytes("UTF-8")
      ehClient.send(new EventData(payloadBytes), key).toScala.map(_ => msg)
  }

}
