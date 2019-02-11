[![Build Status](https://travis-ci.org/navicore/akka-eventhubs.svg?branch=master)](https://travis-ci.org/navicore/akka-eventhubs)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/e25a174959584265a3cbc7242c8acc78)](https://www.codacy.com/app/navicore/akka-eventhubs?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=navicore/akka-eventhubs&amp;utm_campaign=Badge_Grade)

Akka Eventhubs
---

Akka Streams Azure Eventhubs Source and Sink

# USAGE

update your `build.sbt` dependencies with:

```scala
// https://mvnrepository.com/artifact/tech.navicore/akkaeventhubs
libraryDependencies += "tech.navicore" %% "akkaeventhubs" % "1.0.0"
```

# SOURCE

add to `application.conf`

```
eventhubs {

  dispatcher {
    type = Dispatcher
    executor = "thread-pool-executor"
    thread-pool-executor {
      core-pool-size-min = 4
      core-pool-size-factor = 2.0
      core-pool-size-max = 8
    }
    throughput = 10
    mailbox-capacity = -1
    mailbox-type = ""
  }

}

eventhubs-in {

  snapshotInterval = 100

  persist = false

  persistFreq = 1

  offsetPersistenceId = "my_example_eventhubsOffset"

  connection {

    connStr = ${EVENTHUBS_1_CONNSTR}

    partitions = ${EVENTHUBS_1_PARTITION_COUNT}

    defaultOffset = "LATEST"

    consumerGroup = "$Default"

    receiverTimeout = 120s

    receiverBatchSize = 1

    readersPerPartition = 1
  
  }

  dispatcher {
    type = Dispatcher
    executor = "thread-pool-executor"
    thread-pool-executor {
      core-pool-size-min = 4
      core-pool-size-factor = 2.0
      core-pool-size-max = 8
    }
    throughput = 10
    mailbox-capacity = -1
    mailbox-type = ""
  }

}

```

ack the the item once processed for a partition source:

```scala
    val cfg: Config = ConfigFactory.load().getConfig("eventhubs-in")

    val source1 = createPartitionSource(0, cfg)

    source1.runForeach(m => {
        println(s"SINGLE SOURCE: ${m._1.substring(0, 160)}")
        m._2.ack()
    })
```

ack the the item once processed after merging all the partition sources:

```scala
    val consumer: Sink[(String, AckableOffset), Future[Done]] =
        Sink.foreach(m => {
            println(s"SUPER SOURCE: ${m._1.substring(0, 160)}")
            m._2.ack()
        })

    val toConsumer = createToConsumer(consumer)

    val cfg: Config = ConfigFactory.load().getConfig("eventhubs-in")

    for (pid <- 0 until  EventHubConf(cfg).partitions) {

        val src: Source[(String, AckableOffset), NotUsed] =
          createPartitionSource(pid, cfg)

        src.runWith(toConsumer)

    }
```

### With Persistence of Offsets

change `applicagtion.conf` and configure [Actor Persistence]

```
eventhubs-in {
  persist = true
...
...
...
```

# SINK

The sing requires a stream shape using a case class

```
case class EventhubsSinkData(payload: Array[Byte],
                             keyOpt: Option[String] = None,
                             props: Option[Map[String, String]] = None,
                             ackable: Option[AckableOffset] = None,
                             genericAck: Option[() => Unit] = None)
```

* `payload` is what you think it is.
* `keyOpt` is the partition key.  If not set, the Sink will use a hash of the payload.
* `props` is an optional string map that will add properties to the Eventhubs metadata for this item.
* `ackable` is optional and will be committed when the payload is successfully sent.
* `genericAck` is an optional function and will be called when the payload is successfully sent.


```
    val outConfig: Config = ConfigFactory.load().getConfig("eventhubs-out")
...
...
...
      val format = Flow[(String, AckableOffset)].map((x: (String, AckableOffset)) =>
        EventhubsSinkData(x._1.getBytes("UTF8"), None, None, Some(x._2))
      )

      src.via(<SOME_PROCESSING_FLOW>).via(format).runWith(new EventhubsSink(EventHubConf(outConfig)))
```

## OPS

### publish local

```console
sbt +publishLocalSigned
```

### publish to nexus staging

```console
export GPG_TTY=$(tty)
sbt +publishSigned
sbt sonatypeReleaseAll
```

---
[Actor Persistence]:https://doc.akka.io/docs/akka/2.5.4/scala/persistence.html

