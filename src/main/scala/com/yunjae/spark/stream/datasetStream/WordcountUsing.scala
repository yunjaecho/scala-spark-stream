package com.yunjae.spark.stream.datasetStream

import scala.io.Source

object WordcountUsing extends App {
  // Load data
  val lines = Source.fromFile("data/othello.txt").getLines()

  // Count words
  val counts = lines.flatMap(line => line.split("\\s+"))
    .toSeq
    .groupBy(_.toLowerCase)
    .mapValues(_.length)

  // Sort and print top 20
  counts
    .toSeq
    .sortBy(_._2)
    .reverse
    .take(20)
    .foreach(println)
}
