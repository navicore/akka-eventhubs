[![Build Status](https://travis-ci.org/navicore/akka-eventhubs.svg?branch=master)](https://travis-ci.org/navicore/akka-eventhubs)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/e25a174959584265a3cbc7242c8acc78)](https://www.codacy.com/app/navicore/akka-eventhubs?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=navicore/akka-eventhubs&amp;utm_campaign=Badge_Grade)

Akka Eventhubs
---

Akka Streams Azure Eventhubs Source ~~and Sink~~

# STATUS

 * Alpha - under construction
 * does not recover from timeouts yet
 * tested with Akka 2.5.6

# USAGE

update your `build.sbt` dependencies with:

```scala
// https://mvnrepository.com/artifact/tech.navicore/akkaeventhubs
libraryDependencies += "tech.navicore" %% "akkaeventhubs" % "0.1.18"
```

add to `application.conf`

```
eventhubs-1 {

  persist = false
  snapshotInterval = 100
  persistFreq = 1

  offsetPersistenceId = "my_example_eventhubsOffset"

  connection {
    endpoint = ${EVENTHUBS_1_ENDPOINT}
    name = ${EVENTHUBS_1_NAME}
    namespace = ${EVENTHUBS_1_NAMESPACE}
    partitions = ${EVENTHUBS_1_PARTITION_COUNT}
    accessPolicy = ${EVENTHUBS_1_ACCESS_POLICY}
    accessKey = ${EVENTHUBS_1_ACCESS_KEY}
    consumerGroup = "$Default"
    receiverTimeout = 30s
    receiverBatchSize = 1
  }
}
```

// todo: simple 1 partition example:

ack the the item once processed after merging all the partition sources:

```scala
import akka.stream.scaladsl.{Sink, Source}
import akka.{Done, NotUsed}
import com.typesafe.config.{Config, ConfigFactory}
import onextent.akka.ehexample.Conf._
import onextent.akka.eventhubs.Connector.AckableOffset
import onextent.akka.eventhubs.EventHubConf
import onextent.akka.eventhubs.Eventhubs._

import scala.concurrent.Future

object Main extends App {

  val consumer: Sink[(String, AckableOffset), Future[Done]] =
    Sink.foreach(m => {
      println(s"SUPER SOURCE: ${m._1.substring(0, 160)}")
      m._2.ack()
    })

  val toConsumer = createToConsumer(consumer)

  val cfg: Config = ConfigFactory.load().getConfig("eventhubs-1")

  for (pid <- 0 to EventHubConf(cfg).partitions) {

    val src: Source[(String, AckableOffset), NotUsed] =
      createPartitionSource(pid, cfg)

    src.runWith(toConsumer)

  }

}

```

### With Persistence of Offsets

change `applicagtion.conf` and configure [Actor Persistence]

```
eventhubs-1 {
  persist = true
...
...
...
```


## OPS

### publish local

```console
sbt +publishLocalSigned
```

### publish to nexus staging

```console
sbt +publishSigned
sbt sonatypeReleaseAll
```

TODO:

* support multiple partition readers reading the same partition/consumerGroup in different clusters
* support multiple partition readers reading the same partition/consumerGroup in the same cluster

---
[Actor Persistence]:https://doc.akka.io/docs/akka/2.5.4/scala/persistence.html

