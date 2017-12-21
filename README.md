Akka Eventhubs
---

Akka Streams Azure Eventhubs Source ~~and Sink~~

# STATUS

 * Alpha 
 * needs error recovery

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
