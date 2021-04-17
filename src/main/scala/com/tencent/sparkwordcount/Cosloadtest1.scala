package com.tencent.sparkwordcount

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * @author 欧振宇
 * @since 2020/11/29 17:36
 * @version 1.0
 * Copyright(c) 2020/11/29, 作者版权所有.
 */
object Cosloadtest1 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = new SparkSession.Builder()
      .master("local[*]")
      .appName("cos_load_test")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    val lines: RDD[String] = sc.textFile("D:\\data\\input\\qcloud_cos_forbid_log_bak_202006.txt")
    val rdd: RDD[CosLoad] = lines.map(_.split("\t")).map(x =>
      CosLoad(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12),x(13))
    )
    import spark.implicits._
    val df: DataFrame = rdd.toDF()
//    df.show()
//    val rows: Array[Row] = df.collect()
//    df.filter(col("reason") =!= "").select(get_json_object(col("extra"), "$.req.url") as "url")
//    val url: DataFrame = df.select(when(col("reason") === "", get_json_object(col("extra"), "$.req.url") as "url").otherwise(""))
    df.select(get_json_object(col("extra"), "$.req.url")).show()


  }

  case class CosLoad(log_id:String,bucket_id:String,bucket:String,appid:String,uin:String,cos_type:String,status:String,path:String,
                     user:String,	reason:String	,op:String,	source:String	,extra:String	,log_created:String)

}
