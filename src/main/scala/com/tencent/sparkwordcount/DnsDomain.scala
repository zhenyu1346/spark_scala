package com.tencent.sparkwordcount

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

/**
 * @author 欧振宇
 * @since 2020/12/2 22:02
 * @version 1.0
 * Copyright(c) 2020/12/2, 作者版权所有.
 */
object DnsDomain {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("dnsdomain")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    val rdd: RDD[String] = sc.textFile("C:\\Users\\31343\\Desktop\\dns_domain")
    val lines: RDD[DnsDomain] = rdd.map(_.split("\t")).map(x => {
      DnsDomain(x(0), x(1), x(2))
    })
    import spark.implicits._
    val df: DataFrame = lines.toDF()
//    df.show()
    val result: DataFrame = df.withColumn("domains", get_json_object(col("domain_json"), "$.result.domains"))
      .select("uin", "domains")
//    val domain: DataFrame = result.withColumn("domain", regexp_replace(col("domains"), "[\\[\\]\"]", ""))
    val domain: DataFrame = result
      .withColumn("domain", explode(split(regexp_replace(col("domains"),"[\\[\\]\"]",""), ",")))
      .select("uin", "domain")

    domain.coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
//      .format("orc")
      .save("C:\\Users\\31343\\Desktop\\domain_uin")
  }

  case class DnsDomain(ftime:String,uin:String,domain_json:String)
}
