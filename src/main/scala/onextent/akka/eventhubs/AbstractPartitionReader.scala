package onextent.akka.eventhubs

import java.time.Duration
import java.util.concurrent.ScheduledExecutorService

import akka.actor.Actor
import com.microsoft.azure.eventhubs._
import com.typesafe.scalalogging.LazyLogging
import onextent.akka.eventhubs.Connector.Event

abstract class AbstractPartitionReader(partitionId: Int,
                                       eventHubConf: EventHubConf)
    extends Actor
    with LazyLogging {

  import eventHubConf._

  var state: EventPosition =
    if (defaultOffset.equals("LATEST")) {
      EventPosition.fromEndOfStream()
    } else {
      EventPosition.fromStartOfStream()
    }

  import java.util.concurrent.Executors
  val executorService: ScheduledExecutorService =
    Executors.newScheduledThreadPool(eventHubConf.threads)
  val ehClient: EventHubClient =
    EventHubClient.createSync(connStr, executorService)

  lazy val receiver: PartitionReceiver = ehClient.createEpochReceiverSync(
    ehConsumerGroup,
    partitionId.toString,
    state,
    System.currentTimeMillis())

  def initReceiver: () => Unit = () => {
    receiver.setReceiveTimeout(Duration.ofSeconds(20))
  }
  // wheel to call from init
  def read(): List[Event] = {
    var result: List[EventData] = List()
    while (result.isEmpty) {
      val receivedEventsOpt = Option(receiver.receiveSync(ehRecieverBatchSize))
      result = receivedEventsOpt match {
        case Some(receivedEvents) =>
          val itero = Option(receivedEvents.iterator())
          itero match {
            case Some(iter) if iter.hasNext =>
              import scala.collection.JavaConverters._
              val r: List[EventData] = iter.asScala.toList
              logger.debug(
                s"read ${r.length} messages with read batch size of $ehRecieverBatchSize")
              r
            case _ => List()
          }
        case _ => List()
      }
    }
    result.map(eventData => Event(self, partitionId, eventData))
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    logger.info(s"preRestart calling close on ehClient for pid $partitionId")
    ehClient.closeSync()
    super.preRestart(reason, message)
  }
}
