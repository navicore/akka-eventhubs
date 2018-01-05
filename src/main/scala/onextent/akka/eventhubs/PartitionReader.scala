package onextent.akka.eventhubs

import akka.actor.{ActorRef, Props}
import akka.routing.RoundRobinPool
import akka.util.Timeout
import onextent.akka.eventhubs.Connector.Ack

object PartitionReader {

  private def props(partitionId: Int, source: ActorRef, eventHubConf: EventHubConf)(
      implicit timeout: Timeout) =
    Props(new PartitionReader(partitionId, source, eventHubConf))
  val nameBase: String = s"PartitionReader"
  def propsWithDispatcherAndRoundRobinRouter(
      dispatcher: String,
      nrOfInstances: Int,
      partitionId: Int,
      source: ActorRef,
      eventHubConf: EventHubConf)(implicit timeout: Timeout): Props = {
    props(partitionId, source, eventHubConf)
      .withDispatcher(dispatcher)
      .withRouter(RoundRobinPool(nrOfInstances = nrOfInstances))
  }
}

class PartitionReader(partitionId: Int, connector: ActorRef, eventHubConf: EventHubConf)
    extends AbstractPartitionReader(partitionId, connector, eventHubConf)
    with Conf {

  var outstandingAcks = 0
  // kick off a wheel at init
  initReceiver()
  read().foreach(event => {
    outstandingAcks += 1
    connector ! event
  })

  def receive: Receive = receiveCmd

  def receiveCmd: Receive = {

    case ack: Ack =>
      logger.debug(s"partition $partitionId ack for ${ack.offset}")
      if (ack.offset != "") state = ack.offset
      outstandingAcks -= 1
      // kick off a wheel on every ack
      if (outstandingAcks <= 1) {
        read().foreach(event => {
          outstandingAcks += 1
          connector ! event
        })
      }
    case x => logger.error(s"I don't know how to handle ${x.getClass.getName}")

  }
}
