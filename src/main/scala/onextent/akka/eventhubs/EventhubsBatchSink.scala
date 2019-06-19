package onextent.akka.eventhubs

import java.util.concurrent.{Executors, ScheduledExecutorService}

import akka.stream._
import akka.stream.stage._
import com.microsoft.azure.eventhubs.{
  EventData,
  EventHubClient,
  EventHubException
}
import com.typesafe.scalalogging.LazyLogging

/*

  Sink will write a byte array to eh with a partition key of type String.

  If key is not set, a hash will be calculated - it is best to set the key.

 */
class EventhubsBatchSink(eventhubsConfig: EventHubConf, partitionId: Int = 0)
    extends GraphStage[SinkShape[Seq[EventhubsSinkData]]]
    with LazyLogging {

  val executorService: ScheduledExecutorService =
    Executors.newScheduledThreadPool(eventhubsConfig.threads)

  var ehClient: EventHubClient =
    EventHubClient.createSync(eventhubsConfig.connStr, executorService)

  val in: Inlet[Seq[EventhubsSinkData]] = Inlet.create("EventhubsSink.in")

  override def shape(): SinkShape[Seq[EventhubsSinkData]] = SinkShape.of(in)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {

    logger.info(s"eventhub $partitionId create logic")

    new GraphStageLogic(shape()) {

      setHandler(
        in,
        new AbstractInHandler {

          var count: Long = 0 // experimental counter for logging

          override def onPush(): Unit = {

            val elements: Seq[EventhubsSinkData] = grab(in)

            val batch = elements.map(element => {
              val payloadBytes = EventData.create(element.payload)
              element.props.fold()(p =>
                p.keys.foreach(k => payloadBytes.getProperties.put(k, p(k))))
              payloadBytes
            })

            // ejs map to Seq of EventData

            try {
              import collection.JavaConverters._
              ehClient.sendSync(batch.asJava)
              elements.foreach(element => {
                element.ackable.fold()(a => a.ack())
                element.genericAck.fold()(a => a())
                logger.debug(
                  s"eventhubs sink partition $partitionId successfully sent key ${element.keyOpt}, count = $count")
              })
              count += 1
            } catch {
              case ee: EventHubException =>
                logger.error(
                  s"eventhub $partitionId exception: ${ee.getMessage}",
                  ee)
                if (sys.env.getOrElse("AKKA_EH_DIE_ON_ERROR", "") == "YES") {
                  logger.error("FATAL ERROR 3 - ABORT", ee)
                  System.exit(1)
                }
                reConnect()
              case e: Throwable =>
                logger.error(
                  s"eventhub $partitionId unexpected: ${e.getMessage}",
                  e)
                if (sys.env.getOrElse("AKKA_EH_DIE_ON_ERROR", "") == "YES") {
                  logger.error("FATAL ERROR 4 - ABORT", e)
                  System.exit(1)
                }
                reConnect()
            }
            pull(in)

          }

        }
      )

      def reConnect(): Unit = {
        logger.warn("reconnecting sync")
        ehClient.closeSync()
        ehClient =
          EventHubClient.createSync(eventhubsConfig.connStr, executorService)
      }

      override def preStart(): Unit = {
        logger.info(s"starting eventhubs sink")
        pull(in)
        super.preStart()
      }
    }

  }

}
