package onextent.akka.eventhubs

import akka.actor.ActorSystem
import akka.pattern.AskTimeoutException
import akka.serialization.SerializationExtension
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.{Duration, FiniteDuration}

object Conf extends Conf with LazyLogging {

  implicit val actorSystem: ActorSystem = ActorSystem(appName, conf)
  SerializationExtension(actorSystem)

  implicit val ec: ExecutionContextExecutor = actorSystem.dispatcher

  val decider: Supervision.Decider = {

    case _: AskTimeoutException =>
      // might want to try harder, retry w/backoff if the actor is really supposed to be there
      logger.warn(s"decider discarding message to resume processing")
      Supervision.Resume

    case e: java.text.ParseException =>
      logger.warn(
        s"decider discarding unparseable message to resume processing: $e")
      Supervision.Resume

    case e: Throwable =>
      logger.error(s"decider can not decide: $e", e)
      Supervision.Stop

    case e =>
      logger.error(s"decider can not decide: $e")
      Supervision.Stop

  }

  implicit val materializer: ActorMaterializer = ActorMaterializer(
    ActorMaterializerSettings(actorSystem).withSupervisionStrategy(decider))

}

trait Conf {

  val overrides: Config = ConfigFactory.load()

  val conf: Config = overrides.withFallback(ConfigFactory.load())

  val appName: String = conf.getString("main.appName")

  val persist: Boolean = conf.getBoolean("eventhubs-1.persist")
  val persistFreq: Int = conf.getInt("eventhubs-1.persistFreq")
  val offsetPersistenceId: String =
    conf.getString("eventhubs-1.offsetPersistenceId")
  val snapshotInterval: Int = conf.getInt("eventhubs-1.snapshotInterval")
  val ehRecieverBatchSize: Int =
    conf.getInt("eventhubs-1.connection.receiverBatchSize")
  if (ehRecieverBatchSize < persistFreq)
    throw new Exception(
      s"ehRecieverBatchSize $ehRecieverBatchSize is less than persistFreq $persistFreq")
  val ehConsumerGroup: String =
    conf.getString("eventhubs-1.connection.consumerGroup")
  val ehNamespace: String = conf.getString("eventhubs-1.connection.namespace")
  val ehName: String = conf.getString("eventhubs-1.connection.name")
  val ehAccessPolicy: String =
    conf.getString("eventhubs-1.connection.accessPolicy")
  val ehAccessKey: String = conf.getString("eventhubs-1.connection.accessKey")

  val partitions: Int = conf.getInt("eventhubs-1.connection.partitions")
  def requestTimeout: Timeout = {
    val t = conf.getString("eventhubs-1.connection.receiverTimeout")
    val d = Duration(t)
    FiniteDuration(d.length, d.unit)
  }

  implicit val timeout: Timeout = requestTimeout
}
