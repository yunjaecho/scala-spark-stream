package com.yunjae.spark.stream.structured

import com.yunjae.spark.stream.structured.StructuredStreamParseData.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

object SQLStructuredStream extends App {
  val spark = SparkSession.builder().appName("JoinStructuredDataset").master("local[*]").getOrCreate()
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

  // Publish SQL table
  peopleStream.createOrReplaceTempView("peopleTable")

  // SQL query
  val query = spark.sql("SELECT country, avg(age) FROM peopleTable GROUP BY country")

  // Output
  query.writeStream
    .outputMode("complete")
    .format("console")
    .start
    .awaitTermination


}
