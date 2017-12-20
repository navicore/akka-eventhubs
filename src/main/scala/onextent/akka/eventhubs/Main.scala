package onextent.akka.eventhubs

import akka.stream.scaladsl.Source
import Conf._

object Main extends App {

  val sourceGraph = new Eventhubs

  val mySource = Source.fromGraph(sourceGraph)

  mySource.runForeach(m => {

    println(s"ejs yay: ${m._1.substring(0, 140)}")
    m._2.ack()
  })

}
