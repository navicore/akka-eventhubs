package onextent.akka.eventhubs

import akka.util.Timeout
import com.microsoft.azure.eventhubs.ConnectionStringBuilder
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration.{Duration, FiniteDuration}

trait Conf {

  val overrides: Config = ConfigFactory.load()

  val conf: Config = overrides.withFallback(ConfigFactory.load())

  val appName: String = conf.getString("main.appName")


  def requestTimeout: Timeout = {
    val t = conf.getString("eventhubs-1.connection.receiverTimeout")
    val d = Duration(t)
    FiniteDuration(d.length, d.unit)
  }

  implicit val timeout: Timeout = requestTimeout
}

case class EventHubConf(id: Int) extends Conf {
  val readersPerPartition: Int =
    conf.getInt(s"eventhubs-$id.connection.readersPerPartition")
  val persist: Boolean = conf.getBoolean(s"eventhubs-$id.persist")
  val persistFreq: Int = conf.getInt(s"eventhubs-$id.persistFreq")
  val offsetPersistenceId: String =
    conf.getString(s"eventhubs-$id.offsetPersistenceId")
  val snapshotInterval: Int = conf.getInt(s"eventhubs-$id.snapshotInterval")
  val ehRecieverBatchSize: Int =
    conf.getInt(s"eventhubs-$id.connection.receiverBatchSize")
  val ehConsumerGroup: String =
    conf.getString(s"eventhubs-$id.connection.consumerGroup")
  val ehNamespace: String = conf.getString(s"eventhubs-$id.connection.namespace")
  val ehName: String = conf.getString(s"eventhubs-$id.connection.name")
  val ehAccessPolicy: String =
    conf.getString(s"eventhubs-$id.connection.accessPolicy")
  val ehAccessKey: String = conf.getString(s"eventhubs-$id.connection.accessKey")
  val partitions: Int = conf.getInt(s"eventhubs-$id.connection.partitions")
  val connStr: String = new ConnectionStringBuilder(ehNamespace, ehName, ehAccessPolicy, ehAccessKey).toString
  if (ehRecieverBatchSize < persistFreq) throw new Exception( s"ehRecieverBatchSize $ehRecieverBatchSize is less than persistFreq $persistFreq")
}

object EventHubConf1 extends EventHubConf(1)
object EventHubConf2 extends EventHubConf(2)

