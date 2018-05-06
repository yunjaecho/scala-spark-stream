package com.yunjae.spark.stream.datasetStream

import org.apache.spark.sql.SparkSession

object SparkDataset extends App {


    val spark = SparkSession.builder().appName("SparkLession3").master("local[*]").getOrCreate()

    import spark.implicits._

    // Load Data
    val text = spark.read.text("data/othello/part*").as[String]

    // Count words (key value fair)
    val counts = text.flatMap(line => line.split("\\s+"))
      .groupByKey(_.toLowerCase())
      .count()

    /**
      * root
      * |-- value: string (nullable = true)
      * |-- count(1): long (nullable = false)
      */
    counts.printSchema()

    // Display most common
    counts.orderBy($"count(1)"  desc).show()
}
