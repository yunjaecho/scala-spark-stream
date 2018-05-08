package com.yunjae.spark.stream.dstreamComparise

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, State, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

import scala.io.Source

object CustomReceivedDStream extends App {
  val conf = new SparkConf().setAppName("Lession1").setMaster("local[3]")
  val sc = new SparkContext(conf)

  val ssc = new StreamingContext(sc, Seconds(1))

  ssc.receiverStream(timedFileSource("data/summer.txt"))
    .flatMap(_.split("\\s+"))
    .map(word => (word, 1))
    .reduceByKey(_ + _)
    .print()

  ssc.start()
  ssc.awaitTermination()


  // Should be a class ...
  def timedFileSource(fileName: String) = {
    new Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {
      override def onStart(): Unit = {
         new Thread("Timed File Source") {
           override def run(): Unit = {
             receive()
           }
         }.start()
      }

      override def onStop(): Unit = {}

      private def receive(): Unit = {
        for (line <- Source.fromFile(fileName).getLines()) {
          println(line) // print for debugging
          store(line)   // send the line as a source
          Thread.sleep(1000L) // wait one second
        }
      }
    }
  }
}
