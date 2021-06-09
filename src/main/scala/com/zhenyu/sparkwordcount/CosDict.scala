package com.zhenyu.sparkwordcount

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}


/**
 * @author 欧振宇
 * @since 2021/4/15 21:17
 * @version 1.0
 */
object CosDict {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("dns_domain")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    val lines: RDD[String] = sc.textFile("C:\\Users\\31343\\Desktop\\cos_dict")

    // 创建样例类CosDict类型的rdd对象
    val line: RDD[CosDict] = lines.map(_.split("\t")).map(x => {
      CosDict(x(0), x(1), x(2), x(3))
    })

    // rdd转dateFrame,导入隐式转换
    import spark.implicits._
    val df: DataFrame = line.toDF()

    val dataFrame: Dataset[Row] = df.repartition(20)
    dataFrame.show()


    df.coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .save("C:\\Users\\31343\\Desktop\\cos_dict_result")
    val frame: DataFrame = df.withColumn("id", monotonically_increasing_id)
    frame.select("ban_url_union").collect()

  }


  case class CosDict(tdbankImpDate:String,domain:String,url:String,banUrl:String)
}
