package com.tencent.sparkwordcount

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, types}
import org.apache.spark.sql.functions._

/**
 * @author 欧振宇
 * @since 2020/11/14 10:19
 * @version 1.0
 */
object SparkDemo1 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = new SparkSession.Builder()
      .master("local[*]")
      .appName("sparkReadCSV")
      .getOrCreate()
//    spark.conf.set("spark.sql.shuffle.partitions","5")

    val flightData2015: DataFrame = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("D:/data/input/csv/2015-summary.csv")
//      .csv("hdfs://ns1/data/spark_test/csv/2015-summary.csv")
//    ds.sort($"col1", $"col2".desc)
//    flightData2015.sort("count")

    // 创建临时表
    // DEST_COUNTRY_NAME , ORIGIN_COUNTRY_NAME , count
    flightData2015.createOrReplaceTempView("tmp_table")
    val sqlWay: DataFrame = spark.sql("select * from tmp_table ")
//    sqlWay.show()
//    flightData2015.groupBy("DEST_COUNTRY_NAME").count().show()
//    flightData2015.select(max("count"))
//    flightData2015.groupBy("")
    flightData2015
      .groupBy("DEST_COUNTRY_NAME")
      .sum("count")
      .withColumnRenamed("sum(count)","destination_total")
      .sort(desc("destination_total"))
      .limit(5)
      .show()
  }

}
