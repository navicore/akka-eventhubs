package onextent.akka.eventhubs

import akka.actor.{Actor, ActorRef, ActorSystem, DeadLetter, Props}
import akka.pattern.ask
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.typesafe.scalalogging.LazyLogging
import onextent.akka.eventhubs.Conf._
import onextent.akka.eventhubs.Connector._

import scala.concurrent.Await

class Eventhubs(implicit system: ActorSystem)
    extends GraphStage[SourceShape[(String, AckableOffset)]]
    with LazyLogging {

  val out: Outlet[(String, AckableOffset)] = Outlet("EventhubsSource")

  override val shape: SourceShape[(String, AckableOffset)] = SourceShape(out)

  val connector: ActorRef =
    system.actorOf(
      Connector.propsWithDispatcherAndRoundRobinRouter("eventhubs-1.dispatcher",
                                                       1),
      Connector.name)

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
    system.actorOf(Props(new DeadLetterMonitor), name = "DeadLetterMonitor")

  system.eventStream.subscribe(deadLetterMonitorActor, classOf[DeadLetter])

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = {
            try {
              val f = connector ask Pull()
              Await.result(f, timeout.duration) match {
                case Event(from, partitionId, eventData) =>
                  val data = new String(eventData.getBytes)
                  logger.debug(
                    s"key ${eventData.getSystemProperties.getPartitionKey} from partition $partitionId")
                  val ack =
                    Ack(partitionId, eventData.getSystemProperties.getOffset)
                  push(out, (data, AckableOffset(ack, from)))
                case x => logger.error(s"I don't know how to handle success $x")
              }
            } catch {
              case _: java.util.concurrent.TimeoutException =>
                logger.error("pull request timeout")
                //todo: make smarter and less violent
                connector ! RestartMessage()
                onPull() //todo do more than hope the stack doesn't fill
            }
          }
        }
      )
    }

}
