package com.yunjae.spark.stream.structured

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

import sys.process._

object GroupByAggregationStream extends App {
  val spark = SparkSession.builder().appName("GroupByAggregationStream").master("local[*]").getOrCreate()
  import spark.implicits._

  // create schema for parsing data
  val caseSchema = ScalaReflection
    .schemaFor[Person]
    .dataType
    .asInstanceOf[StructType]

  val peopleStream = spark.readStream
    .schema(caseSchema)
    .option("header", true) // Headers are matched to Person properties
    .option("maxFilesPerTrigger", 1)  // each file is read in a separate batch
    .csv("data/people/")  // load a csv file
    .as[Person]
  // [$"country"]  is same ['country]
  peopleStream.groupBy($"country")
    .mean("age")
    .writeStream
    .outputMode("complete")
    .format("console")
    .start
    //.awaitTermination

  peopleStream.groupBy('city)
    .agg(first("country") as "country", count("age"))
    .writeStream
    .outputMode("complete")
    .format("console")
    .start
    .awaitTermination




}
