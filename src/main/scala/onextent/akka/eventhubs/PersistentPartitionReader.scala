package onextent.akka.eventhubs

import java.io.IOException

import akka.actor.{ActorRef, Props}
import akka.persistence.{PersistentActor, SaveSnapshotSuccess, SnapshotOffer}
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

  private def takeSnapshot(): Unit =
    if (lastSequenceNr % snapshotInterval == 0 && lastSequenceNr != 0)
      saveSnapshot(state)

  override def receiveCommand: Receive = {

    case ack: Ack =>
      logger.debug(s"partition $partitionId ack for ${ack.offset}")
      state = ack.offset
      //persistAsync(state) { _ =>
      persist(state) { _ =>
        takeSnapshot()
      }
      // kick off a wheel on every ack - ejs is this the bug machine it seems????
      read() match {
        case Some(event) =>
          logger.debug(s"partition $partitionId new msg")
          connector ! event
        case _ => throw new IOException("no new msg")
      }

    case _:SaveSnapshotSuccess  => logger.debug(s"snapshot persisted for partition $partitionId")

    case x => logger.error(s"I don't know how to handle ${x.getClass.getName}")

  }

  //var state: String = PartitionReceiver.END_OF_STREAM //todo actor persistence
  override def receiveRecover: Receive = {

    // BEGIN DB RECOVERY
    case offset: String => state = offset

    case SnapshotOffer(_, snapshot: String) =>
      state = snapshot
    // END DB RECOVERY
  }
}
