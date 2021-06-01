package com.zhenyu.sparkwordcount

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery

/**
 * @author 欧振宇
 * @since 2020/11/15 23:06
 * @version 1.0
 * Copyright(c) 2020/11/15, 作者版权所有.
 */
object SparkReadCsv {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = new SparkSession.Builder().master("local[*]").appName("SparkReadCsvFile").getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", "5")

    val staticDataFrame: DataFrame = spark.read.format("csv")
      .option("header", value = true)
      .option("interSchema", value = true)
      .load("D:\\data\\input\\retail-data\\by-day\\*.csv")

    staticDataFrame.createOrReplaceTempView("retail_data")
//
    val schema: StructType = staticDataFrame.schema
//    //InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country
//    val result: DataFrame = staticDataFrame.selectExpr("CustomerID",
//      "(UnitPrice * Quantity) as total_cost",
//      "InvoiceDate")
//      .groupBy(col("CustomerId"), window(col("InvoiceDate"), "1 day"))
//      .sum("total_cost").sort(col("sum(total_cost)").desc)
//
//    result.show(5)

    // spark结构化流处理 readStream
    import spark.implicits._
    val streamingDataFrame: DataFrame = spark.readStream
      .schema(schema)
      .option("maxFilePerTrigger", 1)
      .format("csv")
      .option("header", value = true)
      .load("D:\\data\\input\\retail-data\\by-day\\*.csv")

    val streamResult: Dataset[Row] = streamingDataFrame.selectExpr("CustomerId",
      "(UnitPrice * Quantity) as total_cost",
      "InvoiceDate")
      .groupBy($"CustomerId", window($"InvoiceDate", "1 day"))
      .sum("total_cost").orderBy($"sum(total_cost)".desc)

    val query: StreamingQuery = streamResult.writeStream
      .format("console") //表示输出到控制台
      .queryName("customer_purchases") //存入内存的表名称
      .outputMode("complete") //complete表示表中的所有记录
      .start()

    query.awaitTermination()
//    spark.sql(
//      """
//        |select * from customer_purchases
//        |""".stripMargin).show(5)
//
//    spark.stop()
  }
}
