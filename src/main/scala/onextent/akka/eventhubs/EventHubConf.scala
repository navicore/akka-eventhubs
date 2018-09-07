package onextent.akka.eventhubs

import akka.util.Timeout
import com.microsoft.azure.eventhubs.ConnectionStringBuilder
import com.typesafe.config.Config

import scala.concurrent.duration.{Duration, FiniteDuration}

case class EventHubConf(cfg: Config) {
  val persist: Boolean = cfg.getBoolean("persist")
  val persistFreq: Int = cfg.getInt("persistFreq")
  val offsetPersistenceId: String = cfg.getString("offsetPersistenceId")
  val snapshotInterval: Int = cfg.getInt("snapshotInterval")
  val ehRecieverBatchSize: Int = cfg.getInt("connection.receiverBatchSize")
  val ehConsumerGroup: String = cfg.getString("connection.consumerGroup")
  val partitions: Int = cfg.getInt("connection.partitions")


  val connStr: String = if (cfg.hasPath("connection.connStr") &&  cfg.getString("connection.connStr").length > 0) {
    cfg.getString("connection.connStr")
  } else {
    new ConnectionStringBuilder()
    .setNamespaceName(cfg.getString("connection.namespace"))
    .setEventHubName(cfg.getString("connection.name"))
    .setSasKeyName(cfg.getString("connection.accessPolicy"))
    .setSasKey(cfg.getString("connection.accessKey"))
    .toString
  }

  val defaultOffset: String = cfg.getString("connection.defaultOffset") // LATEST or EARLIEST

  def requestDuration: Duration = {
    val t = cfg.getString("connection.receiverTimeout")
    Duration(t)
  }
  implicit def requestTimeout: Timeout = {
    val d = requestDuration
    FiniteDuration(d.length, d.unit)
  }
}
