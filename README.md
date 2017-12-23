[![Build Status](https://travis-ci.org/navicore/akka-eventhubs.svg?branch=master)](https://travis-ci.org/navicore/akka-eventhubs)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/e25a174959584265a3cbc7242c8acc78)](https://www.codacy.com/app/navicore/akka-eventhubs?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=navicore/akka-eventhubs&amp;utm_campaign=Badge_Grade)

Akka Eventhubs
---

Akka Streams Azure Eventhubs Source ~~and Sink~~

# STATUS

 * Alpha - under construction
 * does not recover from timeouts yet

# USAGE

update your `build.sbt` dependencies with:

```scala
"tech.navicore" %% "akkaeventhubs" % "0.1.7"
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

ack the the item once processed:

```scala
import akka.stream.scaladsl.Source
import onextent.akka.eventhubs.Eventhubs
import onextent.akka.eventhubs.Conf._
object Main extends App {
  val sourceGraph = new Eventhubs
  val mySource = Source.fromGraph(sourceGraph)
  mySource.runForeach(m => {
    println(s"yay: ${m._1}")
    m._2.ack()
  })
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

* support multiple instances (explicit config load)
* persistent actor (save offsets)
* support multiple partition readers reading the same partition in same cluster

---
[Actor Persistence]:https://doc.akka.io/docs/akka/2.5.4/scala/persistence.html

