package com.zhenyu.sparkwordcount

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * @author 欧振宇
 * @since 2020/11/15 22:29
 * @version 1.0
 *          Copyright (c) 2020/11/15, 作者版权所有.
 */
object SparkDataSetsDemo1 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = new SparkSession.Builder().master("local[*]").appName("SparkDataSetsDemo").getOrCreate()

    val flightsDF: DataFrame = spark.read.parquet("D:\\data\\input\\parquet\\2010-summary.parquet")

    import spark.implicits._
    val flights: Dataset[Flight] = flightsDF.as[Flight]

    var start1: Long = System.currentTimeMillis()

    // DataSet
    flights.filter(x => x.DEST_COUNTRY_NAME != "Canada")
      .map(x => Flight(x.DEST_COUNTRY_NAME, x.ORIGIN_COUNTRY_NAME, x.count + 5))
      .show()
    println("Dataset: " + (System.currentTimeMillis() - start1))

    var start: Long = System.currentTimeMillis()

    // DataFrame
    flightsDF.where($"DEST_COUNTRY_NAME" =!= "Canada")
      .withColumn("count1", $"count" + 5)
      .select($"DEST_COUNTRY_NAME", $"ORIGIN_COUNTRY_NAME", $"count1").show()

    println("DataFrame: " + (System.currentTimeMillis() - start))
  }

  // 定义样例类
  // DEST_COUNTRY_NAME , ORIGIN_COUNTRY_NAME , count
  case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: BigInt)

}

