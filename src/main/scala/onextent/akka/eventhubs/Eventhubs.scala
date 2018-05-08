package onextent.akka.eventhubs

import akka.{Done, NotUsed}
import akka.actor.{Actor, ActorRef, ActorSystem, DeadLetter, Props}
import akka.pattern.ask
import akka.stream.scaladsl.{MergeHub, RunnableGraph, Sink, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Materializer, Outlet, SourceShape}
import com.microsoft.azure.eventhubs.EventPosition
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import onextent.akka.eventhubs.Connector._

import scala.concurrent.{Await, Future}

/**
  * helper functions to create a multi partition consumer
  */
object Eventhubs {

  def createPartitionSource(partitionId: Int, cfg: Config)(
      implicit s: ActorSystem,
      m: Materializer): Source[(String, AckableOffset), NotUsed] = {
    val sg = new Eventhubs(EventHubConf(cfg), partitionId)
    Source.fromGraph(sg)
  }

  def createToConsumer(consumer: Sink[(String, AckableOffset), Future[Done]])(
      implicit s: ActorSystem,
      m: Materializer): Sink[(String, AckableOffset), NotUsed] = {
    val runnableGraph: RunnableGraph[Sink[(String, AckableOffset), NotUsed]] =
      MergeHub
        .source[(String, AckableOffset)](perProducerBufferSize = 16)
        .to(consumer)
    runnableGraph.run()
  }

}

/**
  * main api
  */
class Eventhubs(eventHubConf: EventHubConf, partitionId: Int)(
    implicit system: ActorSystem)
    extends GraphStage[SourceShape[(String, AckableOffset)]]
    with LazyLogging {

  import eventHubConf._
  val out: Outlet[(String, AckableOffset)] = Outlet("EventhubsSource")

  override val shape: SourceShape[(String, AckableOffset)] = SourceShape(out)

  val connector: ActorRef =
    system.actorOf(
      Connector.propsWithDispatcherAndRoundRobinRouter(s"eventhubs.dispatcher",
                                                       1,
                                                       eventHubConf,
                                                       partitionId),
      Connector.name + "-" + partitionId + eventHubConf.ehName
    )
  connector ! Start()

  class DeadLetterMonitor() extends Actor with LazyLogging {
    override def receive: Receive = {
      case d: DeadLetter =>
        d.message match {
          case a: Ack =>
            logger.error(s"DeadLetterMonitorActor : saw ACK dead letter $a")
          case _ =>
            logger.error(s"DeadLetterMonitorActor : saw dead letter $d")
        }
      case x =>
        logger.error(s"I don't know how to handle ${x.getClass.getName}")
    }
  }

  val deadLetterMonitorActor: ActorRef =
    system.actorOf(Props(new DeadLetterMonitor),
                   name =
                     s"DeadLetterMonitor${eventHubConf.ehName}-$partitionId")

  system.eventStream.subscribe(deadLetterMonitorActor, classOf[DeadLetter])

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = {
            try {
              logger.debug("Pull")
              val f = connector ask Pull()
              Await.result(f, eventHubConf.requestDuration) match {
                case Event(from, pid, eventData) =>
                  val data = new String(eventData.getBytes)
                  logger.debug(
                    s"key ${eventData.getSystemProperties.getPartitionKey} from partition $partitionId")
                  val ack =
                    Ack(pid,
                        EventPosition.fromOffset(
                          eventData.getSystemProperties.getOffset))
                  push(out, (data, AckableOffset(ack, from)))
                case x => logger.error(s"I don't know how to handle success $x")
              }
            } catch {
              case _: java.util.concurrent.TimeoutException =>
                logger.error(s"pull request timeout for partition $partitionId")
                //todo: make smarter and less violent
                connector ! RestartMessage()
                onPull() //todo do more than hope the stack doesn't fill
            }
          }
        }
      )
    }

}
