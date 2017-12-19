package onextent.akka.eventhubs

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.typesafe.scalalogging.LazyLogging
import onextent.akka.eventhubs.Conf._
import onextent.akka.eventhubs.ConnectorActor.{Ack, Event, Pull}

import scala.concurrent.Await

class Eventhubs(implicit system: ActorSystem)
    extends GraphStage[SourceShape[String]]
    with LazyLogging {

  val out: Outlet[String] = Outlet("EventhubsSource")

  override val shape: SourceShape[String] = SourceShape(out)

  val connector: ActorRef =
    system.actorOf(ConnectorActor.props(), ConnectorActor.name)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = {
            logger.debug("got pull poll")
            val f = connector ask Pull()
            Await.result(f, timeout.duration) match {
              case Event(from, partitionId, key, data) =>
                logger.debug(s"got $key from $partitionId")
                push(out, data)
                from ! Ack()
              case x => logger.error(s"I don't know how to handle success $x")
            }
            /*
            f.onComplete((r: Any) => {
              r match {
                case Success(event) =>
                  event match {
                    case Event(from, partitionId, key, data) =>
                      logger.debug(s"got $key from $partitionId")
                      push(out, data)
                      from ! Ack
                    case x => logger.error(s"I don't know how to handle success $x")
                  }
                case x =>
                  logger.error(s"I don't know how to handle $x")
              }

            })
             */
          }
        }
      )
    }

}
