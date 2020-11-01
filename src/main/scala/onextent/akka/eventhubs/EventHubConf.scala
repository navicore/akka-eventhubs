package onextent.akka.eventhubs

import java.util.concurrent.ScheduledExecutorService

import akka.util.Timeout
import com.microsoft.azure.eventhubs.{ConnectionStringBuilder, EventHubClient}
import com.typesafe.config.Config

import scala.concurrent.duration.{Duration, FiniteDuration}

case class EventHubConf(cfg: Config) {
  val persist: Boolean = cfg.getBoolean("persist")
  val persistFreq: Int = cfg.getInt("persistFreq")
  val offsetPersistenceId: String = cfg.getString("offsetPersistenceId")
  val snapshotInterval: Int = cfg.getInt("snapshotInterval")
  val ehReceiverBatchSize: Int = cfg.getInt("connection.receiverBatchSize")
  val ehConsumerGroup: String = cfg.getString("connection.consumerGroup")
  val partitions: Int = cfg.getInt("connection.partitions")
  val threads: Int = if (cfg.hasPath("connection.threads")) {
    cfg.getInt("connection.threads")
  } else {
    2
  }

  val connStrOpt: Option[String] =
    if (cfg.hasPath("connection.connStr") && cfg
          .getString("connection.connStr")
          .length > 0) {
      Some(cfg.getString("connection.connStr"))
    } else if (cfg.hasPath("connection.accessKey")) {
      Some(
        new ConnectionStringBuilder()
          .setNamespaceName(cfg.getString("connection.namespace"))
          .setEventHubName(cfg.getString("connection.name"))
          .setSasKeyName(cfg.getString("connection.accessPolicy"))
          .setSasKey(cfg.getString("connection.accessKey"))
          .toString)
    } else None

  val defaultOffset
    : String = cfg.getString("connection.defaultOffset") // LATEST or EARLIEST

  def requestDuration: Duration = {
    val t = cfg.getString("connection.receiverTimeout")
    Duration(t)
  }
  implicit def requestTimeout: Timeout = {
    val d = requestDuration
    FiniteDuration(d.length, d.unit)
  }

  import java.util.concurrent.Executors
  val executorService: ScheduledExecutorService =
    Executors.newScheduledThreadPool(threads)

  def createClient: EventHubClient = {
    connStrOpt match {
      case Some(connStr) =>
        EventHubClient.createFromConnectionStringSync(connStr, executorService)
      case _ => //todo: via SPN
        throw new UnsupportedOperationException("please implement SPN")
    }
  }

}
