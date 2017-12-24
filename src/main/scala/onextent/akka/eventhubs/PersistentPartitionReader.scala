package onextent.akka.eventhubs

import akka.actor.{ActorRef, Props}
import akka.persistence.{PersistentActor, RecoveryCompleted, SaveSnapshotSuccess, SnapshotOffer}
import akka.util.Timeout
import onextent.akka.eventhubs.Conf._
import onextent.akka.eventhubs.Connector.Ack

object PersistentPartitionReader {

  def props(partitionId: Int, source: ActorRef)(implicit timeout: Timeout) =
    Props(new PersistentPartitionReader(partitionId, source))
  val nameBase: String = s"PersistentPartitionReader"

}

class PersistentPartitionReader(partitionId: Int, connector: ActorRef)
    extends AbstractPartitionReader(partitionId, connector)
    with PersistentActor {

  override def persistenceId: String = offsetPersistenceId + "_" + partitionId

  private def takeSnapshot = () => {
    if (lastSequenceNr % snapshotInterval == 0 && lastSequenceNr != 0) {
      saveSnapshot(state)
    }
  }

  private var persistSeqNr = 0
  private def save = () => {
    persistSeqNr += 1
    if (persistSeqNr % persistFreq == 0) {
      persistSeqNr = 0
      persist(state) { _ =>
        takeSnapshot()
      }
    }
  }

  var outstandingAcks = 0

  override def receiveCommand: Receive = {

    case ack: Ack =>
      logger.debug(s"partition $partitionId ack for ${ack.offset}")
      state = ack.offset
      outstandingAcks -= 1
      save()
      // kick off a wheel when outstanding acks are low
      if (outstandingAcks <= 1) {
        read().foreach(event => {
          outstandingAcks += 1
          connector ! event
        })
      }

    case _: SaveSnapshotSuccess =>
      logger.debug(s"snapshot persisted for partition $partitionId")

    case x => logger.error(s"I don't know how to handle ${x.getClass.getName}")

  }

  override def receiveRecover: Receive = {
    // BEGIN DB RECOVERY
    case offset: String =>
      state = offset
      logger.info(s"recovery for offset $state for partition $partitionId")
    case SnapshotOffer(_, snapshot: String) =>
      state = snapshot
      logger.info(s"recovery snapshot offer for offset $state for partition $partitionId")
    case RecoveryCompleted =>
      // kick off a wheel at init
      logger.info(s"recovery complete at offset $state for partition $partitionId")
      initReceiver()
      read().foreach(event => {
        outstandingAcks += 1
        connector ! event
      })
    // END DB RECOVERY
  }
}
