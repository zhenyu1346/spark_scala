package com.tencent.sparkwordcount

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
    val df: DataFrame = spark.read.format("csv")
      .option("header", value = true)
      .option("interSchema", value = true)
      .load("C:\\Users\\31343\\Desktop\\part-00000-c4a1c943-6dc5-482c-bee7-1444c0123684-c000.csv")

    val result: DataFrame = df.withColumn("product",
                  when(col("domain_is_cdn") === "1","cdn")
                        .when(col("domain_is_cos") === "1","cos")
                        .otherwise(""))
      .withColumn("uin_json",
        when(col("reg_json") =!= "0", get_json_object(col("reg_json"), "$.Response.JsonString"))
          .otherwise("0"))
      .withColumn("customer_name",
        when(col("uin_json") =!= "0",get_json_object(col("uin_json"),"$.account_info.name"))
          .otherwise(""))
      .select("domain","reg_uin","icp_uin","cdn_uin","domain_is_cdn","domain_is_cos","product","customer_name")

    result.coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      //      .format("orc")
      .save("C:\\Users\\31343\\Desktop\\domain_flow")

    result.show()
  }
}
