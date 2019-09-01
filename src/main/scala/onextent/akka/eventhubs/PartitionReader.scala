package onextent.akka.eventhubs

import akka.actor.{ActorRef, Props}
import akka.routing.RoundRobinPool
import akka.util.Timeout
import onextent.akka.eventhubs.Connector.Ack

object PartitionReader {

  private def props(partitionId: Int,
                    seed: Long,
                    source: ActorRef,
                    eventHubConf: EventHubConf)(implicit timeout: Timeout) =
    Props(new PartitionReader(partitionId, seed, source, eventHubConf))
  val nameBase: String = s"PartitionReader"
  def propsWithDispatcherAndRoundRobinRouter(
      dispatcher: String,
      nrOfInstances: Int,
      partitionId: Int,
      seed: Long,
      source: ActorRef,
      eventHubConf: EventHubConf)(implicit timeout: Timeout): Props = {
    props(partitionId, seed, source, eventHubConf)
      .withDispatcher(dispatcher)
      .withRouter(RoundRobinPool(nrOfInstances = nrOfInstances))
  }
}

class PartitionReader(partitionId: Int,
                      seed: Long,
                      connector: ActorRef,
                      eventHubConf: EventHubConf)
    extends AbstractPartitionReader(partitionId, eventHubConf) {

  logger.info("creating PartitionReader")

  var outstandingAcks = 0
  // kick off a wheel at init
  initReceiver()
  try {
    logger.debug(s"partition $partitionId priming pump")
    read().foreach(event => {
      logger.debug(s"partition $partitionId prime pump event")
      outstandingAcks += 1
      connector ! event
    })
  } catch {
    case e: Throwable =>
      logger.warn("can not read: $e", e)
      connector ! e
  }

  def receive: Receive = receiveCmd

  def receiveCmd: Receive = {

    case ack: Ack =>
      logger.debug(s"partition $partitionId ack for ${ack.offset}")
      state = ack.offset
      outstandingAcks -= 1
      // kick off a wheel on every ack
      if (outstandingAcks <= eventHubConf.ehRecieverBatchSize + 1) {
        try {
          read().foreach(event => {
            outstandingAcks += 1
            connector ! event
          })
        } catch {
          case e: Throwable =>
            connector ! e
        }
      } else
        logger.debug(s"skipping read due to $outstandingAcks outstanding acks.")

    case x => logger.error(s"I don't know how to handle ${x.getClass.getName}")

  }

}
