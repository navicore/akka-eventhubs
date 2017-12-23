package onextent.akka.eventhubs

import java.io.IOException
import java.time.Duration

import akka.actor.{Actor, ActorRef}
import com.microsoft.azure.eventhubs._
import com.typesafe.scalalogging.LazyLogging
import onextent.akka.eventhubs.Conf._
import onextent.akka.eventhubs.Connector._

abstract class AbstractPartitionReader(partitionId: Int, connector: ActorRef)
    extends Actor
    with LazyLogging {

  var state: String = PartitionReceiver.END_OF_STREAM //todo actor persistence

  val connStr = new ConnectionStringBuilder(ehNamespace,
                                            ehName,
                                            ehAccessPolicy,
                                            ehAccessKey)

  val ehClient: EventHubClient =
    EventHubClient.createFromConnectionStringSync(connStr.toString)

  lazy val receiver: PartitionReceiver = ehClient.createReceiverSync(
    ehConsumerGroup,
    partitionId.toString,
    state,
    false)

  def initReceiver = () => {
    receiver.setReceiveTimeout(Duration.ofSeconds(20))
  }

  // wheel to call from init
  def read(): Option[Event] = {

    var result: Option[EventData] = None

    while (result.isEmpty) {

      val receivedEvents = receiver.receiveSync(ehRecieverBatchSize)
      result = Some(receivedEvents) match {
        case Some(recEv)
            if Option(recEv.iterator()).isDefined && recEv.iterator().hasNext =>
          val e: EventData = recEv.iterator().next()
          Some(e)
        case _ => None
      }
    }
    result match {
      case Some(eventData) =>
        Some(Event(self, partitionId, eventData))
      case _ =>
        logger.debug(s"no eventData")
        None
    }

  }


}
