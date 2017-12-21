Akka Eventhubs
---

Akka Streams Azure Eventhubs Source ~~and Sink~~

# STATUS

 * Alpha 
 * needs error recovery

# USAGE

update your `build.sbt` dependencies with:

```scala
"tech.navicore" %% "akkaeventhubs" % "0.1.5"
```

add to `application.conf`

```
eventhubs-1 {
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
## OPS

### publish local

```console
sbt +publishLocalSigned
```

### publish to nexus staging

```console
sbt +publishSigned
sbt +sonatypeRelease
 or
sbt sonatypeReleaseAll
```
