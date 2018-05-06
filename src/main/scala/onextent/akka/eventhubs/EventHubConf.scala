package onextent.akka.eventhubs

import akka.util.Timeout
import com.microsoft.azure.eventhubs.ConnectionStringBuilder
import com.typesafe.config.Config

import scala.concurrent.duration.{Duration, FiniteDuration}

case class EventHubConf(cfg: Config) {
  val readersPerPartition: Int =
    cfg.getInt("connection.readersPerPartition")
  val persist: Boolean = cfg.getBoolean("persist")
  val persistFreq: Int = cfg.getInt("persistFreq")
  val offsetPersistenceId: String = cfg.getString("offsetPersistenceId")
  val snapshotInterval: Int = cfg.getInt("snapshotInterval")
  val ehRecieverBatchSize: Int = cfg.getInt("connection.receiverBatchSize")
  val ehConsumerGroup: String = cfg.getString("connection.consumerGroup")
  val ehNamespace: String = cfg.getString("connection.namespace")
  val ehName: String = cfg.getString("connection.name")
  val ehAccessPolicy: String = cfg.getString("connection.accessPolicy")
  val ehAccessKey: String = cfg.getString("connection.accessKey")
  val partitions: Int = cfg.getInt("connection.partitions")
  val connStr: String = new ConnectionStringBuilder()
    .setNamespaceName(ehNamespace)
    .setEventHubName(ehName)
    .setSasKeyName(ehAccessPolicy)
    .setSasKey(ehAccessKey)
    .toString
  if (ehRecieverBatchSize < persistFreq)
    throw new Exception(
      s"ehRecieverBatchSize $ehRecieverBatchSize is less than persistFreq $persistFreq")
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
