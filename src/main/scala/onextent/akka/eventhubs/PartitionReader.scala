package onextent.akka.eventhubs

import java.io.IOException

import akka.actor.{ActorRef, Props}
import akka.util.Timeout
import onextent.akka.eventhubs.Connector.Ack

object PartitionReader {

  def props(partitionId: Int, source: ActorRef)(implicit timeout: Timeout) =
    Props(new PartitionReader(partitionId, source))
  val nameBase: String = s"PartitionReader"

}

class PartitionReader(partitionId: Int, connector: ActorRef)
    extends AbstractPartitionReader(partitionId, connector) {

  // kick off a wheel at init
  initReceiver()
  read() match {
    case Some(event) =>
      connector ! event
    case _ => throw new IOException("no init msg")
  }

  def receive: Receive = receiveCmd

  def receiveCmd: Receive = {

    case ack: Ack =>
      logger.debug(s"partition $partitionId ack for ${ack.offset}")
      state = ack.offset
      // kick off a wheel on every ack
      read() match {
        case Some(event) =>
          logger.debug(s"partition $partitionId new msg")
          connector ! event
        case _ => throw new IOException("no new msg")
      }
    case x => logger.error(s"I don't know how to handle ${x.getClass.getName}")

  }
}
