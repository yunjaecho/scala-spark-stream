package com.yunjae.spark.stream.structured

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

object JoinStructuredDataset extends App {
  val spark = SparkSession.builder().appName("JoinStructuredDataset").master("local[*]").getOrCreate()
  import spark.implicits._

  val users = spark.read
    .option("inferSchema", true)
    .option("header", true)
    .csv("data/users.csv")
    .as[User]

  val transactionSchema = ScalaReflection
    .schemaFor[Transaction]
    .dataType
    .asInstanceOf[StructType]

  val transactionStream = spark.readStream
    .schema(transactionSchema)
    .option("header", true)
    .option("maxFilesPerTrigger", 1)
    .csv("data/transactions/*.csv")
    .as[Transaction]

  val spendingByCountry = transactionStream
    .join(users, users("id") === transactionStream("userid"))
    .groupBy($"country")
    .agg(functions.sum($"cost") as "spending")

  spendingByCountry.writeStream
    .outputMode("complete")
    .format("console")
    .start
    .awaitTermination
}


case class User(id: Int, name: String, email: String, country: String)
case class Transaction(userid: Int, product: String, cost: Double)