package onextent.akka.eventhubs

import java.io.IOException

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
    if (persistSeqNr % persistFreq == 0 && persistSeqNr != 0) {
      persist(state) { _ =>
        takeSnapshot()
      }
    }
  }

  override def receiveCommand: Receive = {

    case ack: Ack =>
      logger.debug(s"partition $partitionId ack for ${ack.offset}")
      state = ack.offset
      save()
      // kick off a wheel on every ack
      read() match {
        case Some(event) =>
          logger.debug(s"partition $partitionId new msg")
          connector ! event
        case _ => throw new IOException("no new msg")
      }

    case _: SaveSnapshotSuccess =>
      logger.info(s"snapshot persisted for partition $partitionId")

    case x => logger.error(s"I don't know how to handle ${x.getClass.getName}")

  }

  override def receiveRecover: Receive = {
    // BEGIN DB RECOVERY
    case offset: String => state = offset
    case SnapshotOffer(_, snapshot: String) =>
      state = snapshot
    case RecoveryCompleted =>
      // kick off a wheel at init
      read() match {
        case Some(event) =>
          connector ! event
        case _ => throw new IOException("no init msg")
      }
    // END DB RECOVERY
  }
}
