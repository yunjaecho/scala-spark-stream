package com.yunjae.spark.stream.structured

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

import sys.process._

object StructuredStreamParseData extends App {

  "cat data/people/1.csv" !

  val spark = SparkSession.builder().appName("StructuredStreamParseData").master("local[*]").getOrCreate()
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

  peopleStream.writeStream
    .outputMode("append") // write results to screen
    .format("console")
    .start
    .awaitTermination
}


case class Person(name: String, city: String, country: String, age: Option[Int])
