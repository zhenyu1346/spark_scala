package com.tencent.sparkwordcount

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, get_json_object, when}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * @author 欧振宇
 * @date 2021/5/11 21:30
 * @version 1.0
 */
object SparkWriteCsv {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = new SparkSession.Builder()
      .master("local[*]").appName("write_csv").getOrCreate()
    //    val df: DataFrame = spark.read.format("csv")
    //      .option("header", value = true)
    //      .option("interSchema", value = true)
    //      .load("C:\\Users\\31343\\Desktop\\part-00000-e1ebb5fb-3fc2-4c72-8b58-d0f69aba6aad-c000.csv")

    val sc: SparkContext = spark.sparkContext
    val lines: RDD[String] = sc.textFile("C:\\Users\\31343\\Desktop\\part-00000-e1ebb5fb-3fc2-4c72-8b58-d0f69aba6aad-c000.csv", 5)

    val line: RDD[CosUin] = lines.map(_.split(",")).map(x => CosUin(x(0), x(1), x(2), x(3), x(4)))

    import spark.implicits._
    val df: DataFrame = line.toDF()

    val result: DataFrame = df.withColumn("product",
      when(col("domain_is_cdn") === "1", "cdn")
        .when(col("domain_is_cos") === "1", "cos")
        .otherwise(""))
      .withColumn("uin_json",
        when(col("reg_json") =!= "0", get_json_object(col("reg_json"), "$.Response.JsonString"))
          .otherwise("0"))
      .withColumn("customer_name",
        when(col("uin_json") =!= "0", get_json_object(col("uin_json"), "$.account_info.name"))
          .otherwise("")).select("domain", "uin", "product", "customer_name")


    df.coalesce(5)
      .write.mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .save("C:\\Users\\31343\\Desktop\\domain_flow")

    result.show()
  }


  case class CosUin(domain: String, uin: String, domain_is_cdn: String, domain_is_cos: String, reg_json: String)

  case class CdnUin(domain: String, domain_is_cdn: String, domain_is_cos: String, domain_cdn_uin: String)
}
