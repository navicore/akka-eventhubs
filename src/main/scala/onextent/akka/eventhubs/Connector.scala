package onextent.akka.eventhubs

import akka.actor.SupervisorStrategy._
import akka.actor._
import akka.routing.RoundRobinPool
import akka.util.Timeout
import com.microsoft.azure.eventhubs.{EventData, EventPosition}
import com.typesafe.scalalogging.LazyLogging
import onextent.akka.eventhubs.Connector.{Event, Pull, Start}

import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.concurrent.duration.Duration

object Connector extends LazyLogging {

  val name: String = "ConnectorActor"
  private def props(eventHubConf: EventHubConf, partitionId: Int, seed: Long)(implicit timeout: Timeout) = Props(new Connector(eventHubConf, partitionId: Int, seed: Long))
  final case class Event(from: ActorRef, partitionId: Int, eventData: EventData)
  final case class Pull()
  final case class Start()
  final case class Ack(partitionId: Int, offset: EventPosition, properties: mutable.Map[String, String], partitionKey: String)
  final case class AckableOffset(ackme: Ack, from: ActorRef) {
    def ack(): Unit = {
      from ! ackme
    }
  }
  def propsWithDispatcherAndRoundRobinRouter(
      dispatcher: String,
      nrOfInstances: Int,
      seed: Long,
      eventHubConf: EventHubConf,
      partitionId: Int)(implicit timeout: Timeout): Props = {
    props(eventHubConf, partitionId, seed)
      .withDispatcher(dispatcher)
      .withRouter(RoundRobinPool(nrOfInstances = nrOfInstances, supervisorStrategy = supervise))
  }
  def supervise: SupervisorStrategy = {
    AllForOneStrategy(maxNrOfRetries = -1, withinTimeRange = Duration.Inf) {
      case e: PostRestartException =>
        logger.error(s"post restart supervise restart due to $e")
        Eventhubs.abort(e)
        Stop
      case e: Exception =>
        logger.error(s"supervise restart due to $e")
        Restart
        logger.error(s"supervise escalate due to $e")
        Eventhubs.abort(e)
        Escalate
      case e  =>
        logger.error(s"supervise escalate due to $e")
        Eventhubs.abort(e)
        Escalate
    }
  }

}

class Connector(eventHubConf: EventHubConf, partitionId: Int, seed: Long) extends Actor with LazyLogging {

  import eventHubConf._

  logger.info(s"creating ConnectorActor $partitionId")


//  var state: (Queue[Event], Queue[ActorRef]) =
//    (Queue[Event](), Queue[ActorRef]())

  var state: (Queue[Any], Queue[ActorRef]) =
    (Queue[Any](), Queue[ActorRef]())

  override def receive: Receive = {

    case _: Start =>
      if (persist) {
        logger.info(s"creating PersistentPartitionReader $partitionId")
        context.system.actorOf(
          PersistentPartitionReader.propsWithDispatcherAndRoundRobinRouter(
            s"eventhubs.dispatcher",
            1,
            partitionId,
            seed,
            self, eventHubConf),
          PersistentPartitionReader.nameBase + "-" + partitionId + "-" + seed)
      } else {
        logger.info(s"creating PartitionReader $partitionId")
        context.system.actorOf(
          PartitionReader.propsWithDispatcherAndRoundRobinRouter(
            s"eventhubs.dispatcher",
            1,
            partitionId,
            seed,
            self,
            eventHubConf),
          PartitionReader.nameBase + partitionId + "-" + seed)
      }

    case error: Throwable =>
      logger.error(s"error ${error.getMessage}")
      throw error
      /*
      val (queue, requests) = state
      state = (queue :+ error, requests)
      while (state._1.nonEmpty && state._2.nonEmpty) {
        val (queue, requests) = state
        val (next, newQueue) = queue.dequeue
        val (requestor, newRequests) = requests.dequeue
        requestor ! next
        state = (newQueue, newRequests)
      }
      */

    case event: Event =>
      // add to queue
      logger.debug(s"event from ${event.partitionId}")
      val (queue, requests) = state
      state = (queue :+ event, requests)
      while (state._1.nonEmpty && state._2.nonEmpty) {
        val (queue, requests) = state
        val (next, newQueue) = queue.dequeue
        val (requestor, newRequests) = requests.dequeue
        requestor ! next
        state = (newQueue, newRequests)
      }

    case _: Pull =>
      // remove from queue
      val (queue, requests) = state
      if (queue.isEmpty) {
        logger.debug("Pull of empty state - waiting...")
        state = (queue, requests :+ sender())
      } else {
        val (next, newQueue) = queue.dequeue
        state = (newQueue, requests)
        sender() ! next
      }

    case x => logger.error(s"I don't know how to handle ${x.getClass.getName}")
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    logger.warn(s"restarting $reason $message")
    super.preRestart(reason, message)
  }
}
