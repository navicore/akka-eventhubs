package onextent.akka.eventhubs

import scala.concurrent.duration._
import java.util.concurrent.{Executors, ScheduledExecutorService}

import scala.concurrent.ExecutionContext.Implicits.global
import akka.stream._
import akka.stream.stage._
import com.microsoft.azure.eventhubs.{EventData, EventHubClient}
import com.typesafe.scalalogging.LazyLogging
import onextent.akka.eventhubs.Connector.AckableOffset

import scala.concurrent.{Future, _}

case class EventhubsSinkData(payload: Array[Byte],
                             keyOpt: Option[String] = None,
                             props: Option[Map[String, String]] = None,
                             ackable: Option[AckableOffset] = None,
                             genericAck: Option[() => Unit] = None)

/*

  Sink will write a byte array to eh with a partition key of type String.

  If key is not set, a hash will be calculated - it is best to set the key.

 */
class EventhubsSink(eventhubsConfig: EventHubConf)
    extends GraphStage[SinkShape[EventhubsSinkData]]
    with LazyLogging {

  val executorService: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor()
  var ehClient: EventHubClient =
    EventHubClient.createSync(eventhubsConfig.connStr, executorService)

  val in: Inlet[EventhubsSinkData] = Inlet.create("EventhubsSink.in")

  override def shape(): SinkShape[EventhubsSinkData] = SinkShape.of(in)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {

    logger.info(s"eventhubs create logic")

    new GraphStageLogic(shape()) {

      setHandler(
        in,
        new AbstractInHandler {

          override def onPush(): Unit = {

            val element: EventhubsSinkData = grab(in)

            val key: String =
              element.keyOpt.getOrElse(element.payload.hashCode().toString)

            val payloadBytes = EventData.create(element.payload)
            element.props.fold()(p =>
              p.keys.foreach(k => payloadBytes.getProperties.put(k, p(k))))

            import scala.compat.java8.FutureConverters._
            val f: Future[Void] = ehClient.send(payloadBytes, key).toScala.map(x => {
              element.ackable.fold()(a => a.ack())
              element.genericAck.fold()(a => a())
              logger.debug(s"eventhubs sink successfully sent key $key")
              x
            })

            try {
              Await.ready(f, 10.seconds)
            } catch {
              case to: TimeoutException =>
                logger.error(s"time out: ${to.getMessage}")
                reConnect()
              case i: InterruptedException =>
                logger.error(s"interrupt: ${i.getMessage}")
                reConnect()
              case e =>
                logger.error(s"unexpected: ${e.getMessage}", e)
                reConnect()
            }
            pull(in)

          }

        }
      )

      def reConnect(): Unit = {
        logger.warn("reconnecting sync")
        ehClient.closeSync()
        ehClient = EventHubClient.createSync(eventhubsConfig.connStr, executorService)
      }

      override def preStart(): Unit = {
        logger.info(s"starting eventhubs sink")
        pull(in)
        super.preStart()
      }
    }

  }

}
