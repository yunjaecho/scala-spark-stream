package com.yunjae.spark.stream.dstreamComparise

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

import scala.io.Source

object CumulativeWordcountDStream extends App {
  Logger.getLogger("org").setLevel(Level.OFF)

  val conf = new SparkConf().setAppName("Lession1").setMaster("local[3]")
  val sc = new SparkContext(conf)

  val ssc = new StreamingContext(sc, Seconds(1))

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

  // Cumulative Wordcount using Spark DStream
  // You cannot create jobs in a started streaming context
  // Consider restarting the kernel
  def updateFn(key: String, value: Option[Int], state: State[Int]) = {
    // update state (statefull!)
    state.update(value.getOrElse(0) + state.getOption().getOrElse(0))

    // result to return
    (key, state.get())
  }

  val spec = StateSpec.function(updateFn _)

  // checkpointing is mandatory
  ssc.checkpoint("_checkpoints")

  ssc.receiverStream(timedFileSource("data/summer.txt"))
    .flatMap(_.split("\\s+"))
    .map(word => (word, 1))
    .mapWithState(spec)
    .print()

  ssc.start()
  ssc.awaitTermination()

}
