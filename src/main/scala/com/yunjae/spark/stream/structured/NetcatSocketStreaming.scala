package com.yunjae.spark.stream.structured

import org.apache.spark.sql.SparkSession

object NetcatSocketStreaming extends App {
  val spark = SparkSession.builder().appName("SparkLession5").master("local[*]").getOrCreate()
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

  // other terminal execute command : nc -lk 12341
  createStream(12341, 10000)


}
