package com.yunjae.spark.stream.datasetStream

import org.apache.spark.{SparkConf, SparkContext}

object WordcountRDD extends App {
  val conf = new SparkConf().setAppName("Lession1").setMaster("local[3]")
  val sc = new SparkContext(conf)

  val lines = sc.textFile("data/othello.txt")

  val counts = (lines.flatMap(line => line.split("\\s+")))
    .map(word => (word.toLowerCase, 1))
    .reduceByKey(_ +_)

  counts.sortBy(_._2, ascending = false)
    .take(20)
    .foreach(println)


}
