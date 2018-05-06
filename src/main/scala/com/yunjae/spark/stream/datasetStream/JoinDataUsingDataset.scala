package com.yunjae.spark.stream.datasetStream

import com.yunjae.spark.stream.datasetStream.SparkDataset.spark
import org.apache.spark.sql.SparkSession

object SparkLession4 extends App {
  import sys.process._

  val spark = SparkSession.builder().appName("SparkLession4").master("local[*]").getOrCreate()
  import spark.implicits._

  Process("more data/users.csv").lineStream.foreach(println)

  //"more data/users.csv"

  val users = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("data/users.csv")
    .as[User]

  users.printSchema()

  val transactions = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("data/transactions/*.csv")
    .as[Transaction]

  transactions.printSchema()

  users.join(transactions, users.col("id") === transactions.col("userId"))
    .groupBy($"name")
    .sum("cost")
    .show()


}

case class User(id: Int, name: String, email: String, country: String)
case class Transaction(userId: Int, product: String, cost: Double)