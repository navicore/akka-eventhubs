package onextent.akka.eventhubs

import scala.collection.immutable.Queue
import akka.actor.{Actor, ActorRef, Props}
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import onextent.akka.eventhubs.ConnectorActor._
import onextent.akka.eventhubs.Conf._

object ConnectorActor {

  val name: String = "ConnectorActor"
  def props()(implicit timeout: Timeout) =
    Props(new ConnectorActor())
  final case class Event(from: ActorRef,
                         partitionId: Int,
                         key: String,
                         data: String)
  final case class Pull()
  final case class Ack()
}

class ConnectorActor() extends Actor with LazyLogging {

  logger.info("creating ConnectorActor")

  // create partition actors
  (0 until partitions).foreach(
    n =>
      context.system.actorOf(PartitionReaderActor.props(n, self),
                             PartitionReaderActor.nameBase + n))

  var state: (Queue[Event], Queue[ActorRef]) =
    (Queue[Event](), Queue[ActorRef]())

  override def receive: Receive = {

    case event: Event =>
      // add to queue
      logger.debug(s"got event from ${event.partitionId}")
      val (queue, requests) = state
      state = (queue :+ event, requests)
      while (state._1.nonEmpty && state._2.nonEmpty) {
        val (queue, requests) = state
        val (next, newQueue) = queue.dequeue
        val (requestor, newRequests) = requests.dequeue
        logger.debug(s"sending event to long-waiting requestor from ${next.partitionId}")
        requestor ! next
        state = (newQueue, newRequests)
      }

    case _: Pull =>
      // remove from queue
      logger.debug(s"got pull request")
      val (queue, requests) = state
      if (queue.isEmpty) {
        state = (queue, requests :+ sender())
      } else {
        val (next, newQueue) = queue.dequeue
        state = (newQueue, requests)
        logger.debug(s"sending event to short-waiting requestor from ${next.partitionId}")
        sender() ! next
      }

    case x => logger.error(s"I don't know how to handle ${x.getClass.getName}")
  }

}
