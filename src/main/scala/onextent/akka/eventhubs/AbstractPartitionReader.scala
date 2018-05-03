package onextent.akka.eventhubs

import java.time.Duration

import akka.actor.Actor
import com.microsoft.azure.eventhubs._
import com.typesafe.scalalogging.LazyLogging
import onextent.akka.eventhubs.Connector.Event

abstract class AbstractPartitionReader(partitionId: Int,
                                       eventHubConf: EventHubConf)
    extends Actor
    with LazyLogging {

  import eventHubConf._

  var state: EventPosition = EventPosition.fromEndOfStream()

  import java.util.concurrent.{ExecutorService, Executors}

  val executorService: ExecutorService = Executors.newSingleThreadExecutor
  val ehClient: EventHubClient =
    EventHubClient.createSync(connStr, executorService)

  lazy val receiver: PartitionReceiver = ehClient.createEpochReceiverSync(
    ehConsumerGroup,
    partitionId.toString,
    state,
    1)

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
              iter.asScala.toList
            case _ => List()
          }
        case _ => List()
      }
    }
    result.map(eventData => Event(self, partitionId, eventData))
  }

}
