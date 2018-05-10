package com.yunjae.spark.stream.strandalone

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MainApp  {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MeetupStreaming")
    val ssc = new StreamingContext(conf, Seconds(1))
    val meetupStream = MeetupDStream(ssc)

    args match {
      case Array() =>
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)
        meetupStream.print
      case Array(output) =>
        meetupStream.saveAsTextFiles(s"output/$output")
      case _ => throw new IllegalArgumentException("Expecting at most one an")
    }

    ssc.start()
    ssc.awaitTermination()
  }


}
