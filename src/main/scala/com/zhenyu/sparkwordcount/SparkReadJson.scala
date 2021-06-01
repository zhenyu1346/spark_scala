package com.zhenyu.sparkwordcount

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.get_json_object
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

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

    val lines: RDD[String] = sc.textFile("C:\\Users\\31343\\Desktop\\mb_board")

    val line: RDD[MeetBord] = lines.map(x => x.split("\t")).map(x => MeetBord(x(0)))

    import spark.implicits._
    val df: DataFrame = line.toDF()

    val result: DataFrame = df.withColumn("content_json", get_json_object($"content", "$.CommentSingle"))
      .filter($"content_json".isNotNull).select("content_json")

    result.coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .save("C:\\Users\\31343\\Desktop\\meet_bord")

    result.show()




  }

  case class MeetBord(Content:String)
}
