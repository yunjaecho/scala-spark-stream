package com.yunjae.spark.stream.datasetStream

import org.apache.spark.sql.SparkSession

object StructuredStreamWordcount extends App {
  val spark = SparkSession.builder().appName("SparkLession5").master("local[*]").getOrCreate()
  import spark.implicits._

  // Load data
  val text = spark.readStream
    /**
      * maxFilesPerTrigger
      * maxFilesPerTrigger option specifies the maximum number of files per trigger (batch).
      * It limits the file stream source to read the maxFilesPerTrigger number of files specified at a time and hence enables rate limiting.
      *
      * It allows for a static set of files be used like a stream for testing as the file set is processed maxFilesPerTrigger number of files at a time.
      */
    .option("maxFilesPerTrigger", 1)
    .text("data/othello/part*")
    .as[String]

  // Count words
  val counts = text.flatMap(line => line.split("\\s+"))
    .groupByKey(_.toLowerCase)
    .count

  // Print counts
  val query = counts
    .orderBy($"count(1)" desc)
    .writeStream
    .outputMode("complete")
    .format("console")
    .start

  query.awaitTermination()
}
