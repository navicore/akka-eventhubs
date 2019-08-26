package onextent.akka.eventhubs

import java.lang
import java.time.Duration
import java.util.concurrent.{CompletableFuture, ScheduledExecutorService}

import akka.actor.Actor
import com.microsoft.azure.eventhubs._
import com.typesafe.scalalogging.LazyLogging
import onextent.akka.eventhubs.Connector.Event

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

abstract class AbstractPartitionReader(partitionId: Int,
                                       eventHubConf: EventHubConf)
    extends Actor
    with LazyLogging {

  import eventHubConf._

  var state: EventPosition =
    if (defaultOffset.equals("LATEST")) {
      EventPosition.fromEndOfStream()
    } else {
      EventPosition.fromStartOfStream()
    }

  import java.util.concurrent.Executors
  val executorService: ScheduledExecutorService =
    Executors.newScheduledThreadPool(eventHubConf.threads)
  val ehClient: EventHubClient =
    EventHubClient.createFromConnectionStringSync(connStr, executorService)

  lazy val receiver: PartitionReceiver = ehClient.createEpochReceiverSync(
    ehConsumerGroup,
    partitionId.toString,
    state,
    System.currentTimeMillis())

  def initReceiver: () => Unit = () => {
    receiver.setReceiveTimeout(Duration.ofSeconds(20))
  }

  def read(): List[Event] = {
    var result: List[EventData] = List()
    while (result.isEmpty) {
      import scala.compat.java8.FutureConverters._
      import scala.concurrent.duration._
      val cs: CompletableFuture[lang.Iterable[EventData]] =
        receiver.receive(ehRecieverBatchSize)
      val sf: Future[lang.Iterable[EventData]] = toScala(cs)
      implicit def ec: ExecutionContext = ExecutionContext.global
      val tryresult: Try[lang.Iterable[EventData]] =
        Await.ready(sf, 60.seconds).value.get
      result = tryresult match {
        case Success(r) if r != null=>
          val itero = Option(r.iterator())
          itero match {
            case Some(iter) if iter.hasNext =>
              import scala.collection.JavaConverters._
              val r: List[EventData] = iter.asScala.toList
              logger.debug(
                s"read ${r.length} java future messages with read batch size of $ehRecieverBatchSize")
              r
            case _ => List()
          }
        case Success(_) =>
          logger.debug(s"read failed - null result, non-fatal")
          //throw new java.io.IOException("read failed - null result")
          List()
        case Failure(e) =>
          logger.error(s"read failed due to $e", e)
          throw e
      }
    }
    result.map(eventData => Event(self, partitionId, eventData))
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    logger.info(s"preRestart calling close on ehClient for pid $partitionId")
    ehClient.closeSync()
    super.preRestart(reason, message)
  }
}
