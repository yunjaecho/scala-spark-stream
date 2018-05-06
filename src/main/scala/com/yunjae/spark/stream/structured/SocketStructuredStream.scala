package com.yunjae.spark.stream.structured

import com.yunjae.spark.stream.structured.NetcatSocketStreaming.spark

import sys.process._
import org.apache.spark.sql.SparkSession

object SocketStructuredStream extends App {


  val spark = SparkSession.builder().appName("SparkLession4").master("local[*]").getOrCreate()
  import spark.implicits._


  def createStream(port: Int, duration: Int): Unit = {
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", port)
      .load

    val words = lines.as[String]
      .flatMap(_.split("\\s+"))

    val wordCounts = words
      .groupByKey(_.toLowerCase)
      .count()
      .orderBy($"count(1)" desc)

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start
      .awaitTermination
  }

  //Process("more data/summer.txt").lineStream.foreach(println)
  // run bash command using bang after a string
  "more data/summer.txt" !

  val port = 12342

  new Thread {
    override def run(): Unit = {
      Broadcast.main(Array(port.toString,  "data/summer.txt"))

      //s"scala Broadcast.scala ${port} data/summer.txt" !
    }
  }.start()

  createStream(port, 12000)
}
