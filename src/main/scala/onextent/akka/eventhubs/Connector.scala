package onextent.akka.eventhubs

import akka.actor.SupervisorStrategy._
import akka.actor.{Actor, ActorRef, AllForOneStrategy, PostRestartException, Props, SupervisorStrategy}
import akka.routing.RoundRobinPool
import akka.util.Timeout
import com.microsoft.azure.eventhubs.{EventData, EventPosition}
import com.typesafe.scalalogging.LazyLogging
import onextent.akka.eventhubs.Connector.{Event, Pull, RestartMessage, Start}

import scala.collection.immutable.Queue
import scala.concurrent.duration.Duration

object Connector extends LazyLogging {

  val name: String = "ConnectorActor"
  private def props(eventHubConf: EventHubConf, partitionId: Int)(implicit timeout: Timeout) = Props(new Connector(eventHubConf, partitionId: Int))
  final case class Event(from: ActorRef, partitionId: Int, eventData: EventData)
  final case class Pull()
  final case class Start()
  final case class RestartMessage()
  final case class Ack(partitionId: Int, offset: EventPosition)
  final case class AckableOffset(ackme: Ack, from: ActorRef) {
    def ack(): Unit = {
      from ! ackme
    }
  }
  def propsWithDispatcherAndRoundRobinRouter(
      dispatcher: String,
      nrOfInstances: Int,
      eventHubConf: EventHubConf,
      partitionId: Int)(implicit timeout: Timeout): Props = {
    props(eventHubConf, partitionId)
      .withDispatcher(dispatcher)
      .withRouter(RoundRobinPool(nrOfInstances = nrOfInstances, supervisorStrategy = supervise))
  }
  def supervise: SupervisorStrategy = {
    AllForOneStrategy(maxNrOfRetries = -1, withinTimeRange = Duration.Inf) {
      case e: PostRestartException =>
        logger.error(s"post restart supervise restart due to $e")
        Stop
      case e: Exception =>
        logger.error(s"supervise restart due to $e")
        Restart
      case e  =>
        logger.error(s"supervise escalate due to $e")
        Escalate
    }
  }

}

class Connector(eventHubConf: EventHubConf, partitionId: Int) extends Actor with LazyLogging {

  import eventHubConf._

  logger.info(s"creating ConnectorActor $partitionId")


  var state: (Queue[Event], Queue[ActorRef]) =
    (Queue[Event](), Queue[ActorRef]())

  override def receive: Receive = {

    case _: Start =>
      if (persist) {
        logger.info(s"creating PersistentPartitionReader $partitionId")
        context.system.actorOf(
          PersistentPartitionReader.propsWithDispatcherAndRoundRobinRouter(
            s"eventhubs.dispatcher",
            1,
            partitionId,
            self, eventHubConf),
          PersistentPartitionReader.nameBase + "-" + partitionId + "-" + eventHubConf.ehName)
      } else {
        logger.info(s"creating PartitionReader $partitionId")
        context.system.actorOf(
          PartitionReader.propsWithDispatcherAndRoundRobinRouter(
            s"eventhubs.dispatcher",
            1,
            partitionId,
            self, eventHubConf),
          PartitionReader.nameBase + partitionId + eventHubConf.ehName)
      }

    case event: Event =>
      // add to queue
      logger.debug(s"event from ${event.partitionId}")
      val (queue, requests) = state
      state = (queue :+ event, requests)
      while (state._1.nonEmpty && state._2.nonEmpty) {
        val (queue, requests) = state
        val (next, newQueue) = queue.dequeue
        val (requestor, newRequests) = requests.dequeue
        logger.debug(
          s"sending to waiting requestor from ${next.partitionId}. r q sz: ${requests.size}")
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
        logger.debug(
          s"sending to requestor from ${next.partitionId} q sz: ${queue.size}")
        sender() ! next
      }
    case _: RestartMessage =>
      throw new Exception("restart")

    case x => logger.error(s"I don't know how to handle ${x.getClass.getName}")
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    logger.warn(s"restarting $reason $message")
    super.preRestart(reason, message)
  }
}
