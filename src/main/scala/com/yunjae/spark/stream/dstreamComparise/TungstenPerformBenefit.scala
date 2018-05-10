package com.yunjae.spark.stream.dstreamComparise

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

object TungstenPerformBenefit extends App {
  Logger.getLogger("org").setLevel(Level.OFF)

  val conf = new SparkConf().setAppName("Lession1").setMaster("local[3]")
  val sc = new SparkContext(conf)

  // Using RDDs
  val million = sc.parallelize(0 until math.pow(10, 6).toInt)

  // Using Tungsten
  val count = million.cache().count()
  print(count)

  // Viewing Query Plans in Spark Shell
  // For an RDD, we can view the query plan calling the .toDebugString method. Notice that multiple
  // (even consecutive!) maps and filters are run as separate map and filter steps.
  val resultStr = million.map(_ + 1)
    .map(_ + 1)
    .filter(_ > 1)
    .filter(_ > 2 * 4)
    .toDebugString

  println(resultStr)


  // Viewing Spark Catalyst-Optimized Physical Plans
  // For a dataset, we can view the query plan with the .explain method. You can see that it's
  // performed several optimizations
  //  1. It's grouped multiple (even no-consecutive!) select and filter operations together
  //  2. It's pushed the .filter earlier in the query
  //  3. It performs constant folding by multiplying out 3 * 4
  // Using RDDs

}
