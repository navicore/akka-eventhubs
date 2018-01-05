package onextent.akka.eventhubs

import java.time.Duration

import akka.actor.{Actor, ActorRef}
import com.microsoft.azure.eventhubs._
import com.typesafe.scalalogging.LazyLogging
import onextent.akka.eventhubs.Connector.Event

abstract class AbstractPartitionReader(partitionId: Int, connector: ActorRef, eventHubConf: EventHubConf)
    extends Actor
    with LazyLogging {

  import eventHubConf._

  var state: String = PartitionReceiver.END_OF_STREAM //todo actor persistence

  val ehClient: EventHubClient =
    EventHubClient.createFromConnectionStringSync(connStr.toString)

  lazy val receiver: PartitionReceiver = ehClient.createReceiverSync(
    ehConsumerGroup,
    partitionId.toString,
    state,
    false)

  def initReceiver: () => Unit = () => {
    receiver.setReceiveTimeout(Duration.ofSeconds(20))
  }

  // wheel to call from init
  def read(): List[Event] = {

    var result: List[EventData] = List()

    while (result.isEmpty) {
      val receivedEvents = receiver.receiveSync(ehRecieverBatchSize)
      result = Some(receivedEvents) match {
        case Some(recEv) if Option(recEv.iterator()).isDefined && recEv.iterator().hasNext =>
          import scala.collection.JavaConverters._
          recEv.iterator().asScala.toList
        case _ => List()
      }
    }
    result.map(eventData => Event(self, partitionId, eventData))
  }

}
