package onextent.akka.eventhubs

import java.io.IOException
import java.time.Duration

import akka.actor.{Actor, ActorRef, Props}
import akka.util.Timeout
import com.microsoft.azure.eventhubs.{EventData, PartitionReceiver}
import com.typesafe.scalalogging.LazyLogging
import onextent.akka.eventhubs.Conf._
import onextent.akka.eventhubs.ConnectorActor._

object PartitionReaderActor {

  def props(partitionId: Int, source: ActorRef)(implicit timeout: Timeout) =
    Props(new PartitionReaderActor(partitionId, source))
  val nameBase: String = s"PartitionReaderActor"

}

// todo: add offset to retruning acks
// todo: make persistent actor whose state is based on acks offset data (store the highest/latest ack)
// todo: make acks flow like kafka CommittableOffset mechanism from Flow/Sink ops
// todo: batch reads
class PartitionReaderActor(partitionId: Int, connector: ActorRef)
    extends Actor
    with LazyLogging {

  logger.info(s"creating PartitionReaderActor $partitionId")

  import com.microsoft.azure.eventhubs.{ConnectionStringBuilder, EventHubClient}

  val connStr = new ConnectionStringBuilder(ehNamespace,
                                            ehName,
                                            ehAccessPolicy,
                                            ehAccessKey)

  val ehClient: EventHubClient =
    EventHubClient.createFromConnectionStringSync(connStr.toString)

  val receiver: PartitionReceiver = ehClient.createReceiverSync(
    ehConsumerGroup,
    partitionId.toString,
    PartitionReceiver.END_OF_STREAM,
    false)

  receiver.setReceiveTimeout(Duration.ofSeconds(20))

  // wheel to call from init
  def read(): Option[Event] = {

    var result: Option[String] = None //ejs todo:

    while (result.isEmpty) {

      val receivedEvents = receiver.receiveSync(ehRecieverBatchSize)
      result = Some(receivedEvents) match {
        case Some(recEv) if recEv.iterator().hasNext =>
          val e: EventData = recEv.iterator().next()
          logger.debug(s"read partition $partitionId got EventData")
          Some(new String(e.getBytes))
        case _ => None
      }
    }
    result match {
      case Some(r) =>
        logger.debug(s"read partition $partitionId got result")
        Some(Event(self, partitionId, "", new String(r.getBytes)))
      case _ =>
        None
    }

  }

  // kick off a wheel at init
  read() match {
    case Some(event) =>
      connector ! event
    case _ => throw new IOException("no init msg")
  }

  override def receive: Receive = {

    case _: Ack =>
      logger.debug(s"partition $partitionId got ack")
      // kick off a wheel on every ack
      read() match {
        case Some(event) =>
          logger.debug(s"partition $partitionId got new msg")
          connector ! event
        case _ => throw new IOException("no new msg")
      }
    case x => logger.error(s"I don't know how to handle ${x.getClass.getName}")

  }

}
