package onextent.akka.eventhubs

import akka.stream.scaladsl.Source
import Conf._

object Main extends App {

  //val sourceGraph = new NumberSource
  val sourceGraph = new Eventhubs

  // Create a Source from the Graph to access the DSL
  val mySource = Source.fromGraph(sourceGraph)

  mySource.runForeach(m => println(s"ejs yay: $m"))
  // Returns 55
//  val result1 = mySource.take(10).runFold("")(_ + _)
//  result1.map(println)
//
//  // The source is reusable. This returns 5050
//  val result2 = mySource.take(100).runFold("")(_ + _)
//  result2.map(println)

}
