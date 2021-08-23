package com.zhenyu.sparkwordcount

import org.apache.spark.sql.types.{LongType, Metadata, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author 欧振宇
 * @date 2021/6/10 22:27
 * @version 1.0
 */
object SparkReadJson2 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("spark_read_json")
      .getOrCreate()

    val myManualSchema: StructType = StructType(Array(
      StructField("DEST_COUNTRY_NAME", StringType, nullable = true),
      StructField("ORIGIN_COUNTRY_NAME", StringType, nullable = true),
      StructField("count", LongType, nullable = false, Metadata.fromJson("{\"hello\":\"word\"}"))
    ))

    val df: DataFrame = spark.read.format("json").schema(myManualSchema).load("D:\\data\\input\\json\\2015-summary.json")

    val schema: StructType = df.schema

    println(df.columns.mkString("Array(", ", ", ")"))

    print(schema)

    df.printSchema()

  }
}
