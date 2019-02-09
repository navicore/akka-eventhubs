package onextent.akka.eventhubs

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.stream.scaladsl.{MergeHub, RunnableGraph, Sink, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Materializer, Outlet, SourceShape}
import akka.{Done, NotUsed}
import com.microsoft.azure.eventhubs.EventPosition
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import onextent.akka.eventhubs.Connector._

import scala.concurrent.{Await, Future, TimeoutException}

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

  private def initConnector(): ActorRef = {
    val seed: Long = System.currentTimeMillis()
    val c: ActorRef = system.actorOf(
      Connector.propsWithDispatcherAndRoundRobinRouter(s"eventhubs.dispatcher",
                                                       1,
                                                       seed,
                                                       eventHubConf,
                                                       partitionId),
      Connector.name + "-" + partitionId + "-" + seed
    )
    c ! Start()
    c
  }

  var connector: ActorRef = initConnector()

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
                  import collection.JavaConverters._
                  eventData.getSystemProperties.getPartitionKey
                  val ack =
                    Ack(pid,
                        EventPosition.fromOffset(
                          eventData.getSystemProperties.getOffset),
                        eventData.getProperties.asScala.map(x => (x._1, x._2.toString)),
                        eventData.getSystemProperties.getPartitionKey)
                  push(out, (data, AckableOffset(ack, from)))
                case x => logger.error(s"I don't know how to handle success $x")
              }
            } catch {
              case _: TimeoutException =>
                logger.warn(
                  s"pull request timeout for partition $partitionId. restarting...")
                system.stop(connector) // don't wait for queue to clear
                System.exit(0)
                //connector = initConnector()
                //onPull()
              case e =>
                logger.error(
                  s"pull request exception '${e.getMessage}' for partition $partitionId. restarting...", e)
                system.stop(connector) // don't wait for queue to clear
                System.exit(0)
                //connector = initConnector()
                //onPull()
            }
          }
        }
      )
    }

}
