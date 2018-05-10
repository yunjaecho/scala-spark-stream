package com.yunjae.spark.stream.dstreamComparise

import com.yunjae.spark.stream.dstreamComparise.TungstenPerformBenefit.sc
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ViewCatalystOptimized extends App {
  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession.builder().appName("SparkLession5").master("local[*]").getOrCreate()
  import spark.implicits._

  val million = spark.createDataset((0 until math.pow(10, 6).toInt).toList)

  million.printSchema()

  /**
    * =======================
    * Optimize Query
    * =======================
    * == Physical Plan ==
    * *(1) Project [(value#1 + 3) AS value2#9]
    * +- *(1) Filter ((value#1 >= 1) && (value#1 >= 8))
    * +- LocalTableScan [value#1]
    */
  million
    .select('value,  'value + 1 as 'value2)
    .select('value2 + 1 as 'value2)
    .filter('value >= 1)
    .select('value2 + 1 as 'value2)
    .filter('value >= 2 * 4)
    .explain
}
