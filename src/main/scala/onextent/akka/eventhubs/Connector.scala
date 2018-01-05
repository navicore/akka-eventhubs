package onextent.akka.eventhubs

import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.actor.{Actor, ActorRef, AllForOneStrategy, Props, SupervisorStrategy}
import akka.routing.RoundRobinPool
import akka.util.Timeout
import com.microsoft.azure.eventhubs.EventData
import com.typesafe.scalalogging.LazyLogging
import onextent.akka.eventhubs.Connector.{Event, Pull, RestartMessage}

import scala.collection.immutable.Queue
import scala.concurrent.duration.Duration

object Connector extends LazyLogging {

  val name: String = "ConnectorActor"
  private def props(eventHubConf: EventHubConf)(implicit timeout: Timeout) = Props(new Connector(eventHubConf))
  final case class Event(from: ActorRef, partitionId: Int, eventData: EventData)
  final case class Pull()
  final case class RestartMessage()
  final case class Ack(partitionId: Int, offset: String)
  final case class AckableOffset(ackme: Ack, from: ActorRef) {
    def ack(): Unit = {
      from ! ackme
    }
  }
  def propsWithDispatcherAndRoundRobinRouter(
      dispatcher: String,
      nrOfInstances: Int,
      eventHubConf: EventHubConf)(implicit timeout: Timeout): Props = {
    props(eventHubConf)
      .withDispatcher(dispatcher)
      .withRouter(RoundRobinPool(nrOfInstances = nrOfInstances, supervisorStrategy = supervise))
  }
  def supervise: SupervisorStrategy = {
    AllForOneStrategy(maxNrOfRetries = -1, withinTimeRange = Duration.Inf) {
      case e: Exception =>
        logger.error(s"ejs *************************** supervise restart due to $e")
        Restart
      case e  =>
        logger.error(s"supervise escalate due to $e")
        Escalate
    }
  }

}

class Connector(eventHubConf: EventHubConf) extends Actor with LazyLogging {

  import eventHubConf._

  logger.info("creating ConnectorActor")

  // create partition actors
  (0 until partitions).foreach(
    n =>
      if (persist) {
        logger.info(s"creating PersistentPartitionReader $n")
        context.system.actorOf(
          PersistentPartitionReader.propsWithDispatcherAndRoundRobinRouter(
            "eventhubs-1.dispatcher",
            1,
            n,
            self, eventHubConf),
          PersistentPartitionReader.nameBase + n + eventHubConf.ehName)
      } else {
        logger.info(s"creating PartitionReader $n")
        context.system.actorOf(
          PartitionReader.propsWithDispatcherAndRoundRobinRouter(
            "eventhubs-1.dispatcher",
            1,
            n,
            self, eventHubConf),
          PartitionReader.nameBase + n + eventHubConf.ehName)
    }
  )

  var state: (Queue[Event], Queue[ActorRef]) =
    (Queue[Event](), Queue[ActorRef]())

  override def receive: Receive = {

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
