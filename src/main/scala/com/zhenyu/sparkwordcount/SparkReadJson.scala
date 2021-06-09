package com.zhenyu.sparkwordcount

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, from_json, get_json_object}
import org.apache.spark.sql.types.{ArrayType, DataType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import java.sql.Struct

/**
 * @author 欧振宇
 * @date 2021/5/12 22:25
 * @version 1.0
 */
object SparkReadJson {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("spark_json")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    val lines: RDD[String] = sc.textFile("C:\\Users\\31343\\Desktop\\json_test")

    val line: RDD[MeetBord] = lines.map(x => x.split("\t")).map(x => MeetBord(x(0)))

    import spark.implicits._
    val jsonSchema: StructType = new StructType()
      .add("req", new StructType()
        .add("appid", StringType)
        .add("bucket", StringType)
        .add("url", StringType)
        .add("op", StringType)
        .add("region", StringType)
        .add("clientIp", StringType))
      .add("res", StringType)


        val ds: Dataset[MeetBord] = line.toDS()


        ds.select(from_json($"content", jsonSchema)).as("json_string")
          .select($"json_string.*")
          .select()
          .show()
    //    df.show()
    //
    //    val result: DataFrame = df.withColumn("content_json", get_json_object($"content", "$.CommentSingle"))
    //      .filter($"content_json".isNotNull).select("content_json")
    //
    //    result.coalesce(1)
    //      .write.mode(SaveMode.Overwrite)
    //      .format("com.databricks.spark.csv")
    //      .save("")

    //    df.show()
  }

  case class MeetBord(content: String)

  case class ContentBord(appId: String, bucket: String, url: String, op: String, region: String, clientIp: String)
}
