package com.zhenyu.sparkwordcount

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
 * @author 欧振宇
 * @since 2021/4/15 21:17
 * @version 1.0
 */
object CosDict {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("dnsdomain")
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
    df.show()

//    val rdd1: RDD[Char] = lines.map(x => x.split("\t")).flatMap(x => x(0))
//    val rdd2: RDD[Char] = lines.map(x => x.split("\t")).flatMap(x => x(1))
//    val list1: Array[Char] = rdd1.collect()
//    val list2: Array[Char] = rdd2.collect()
//    val unionRdd: RDD[Char] = rdd1.union(rdd2).distinct()
//    val pairRdd: RDD[(Char, Int)] = unionRdd.map(x => (x, 0)).reduceByKey(_+_)
//    val i: Int = unionRdd.collect().length()
//    pairRdd.foreach(x => {
//      var b = 0
//      for (a <- 0 to i) {
//        if (x._1 == list1(a)) {
//          b += 1
//        }
//      }
//    })
//    val lines: RDD[CosDict] = rdd.map(x => x.split("\t")).map(x => {
//      CosDict(x(0),x(1))
//    })
//    import spark.implicits._
//    val value: RDD[CosDictVector] = lines.map(x => CosDictVector(x.url.split("").mkString("", ", ", ""), x.ban_url.split("").mkString("", ", ", ""),x.url.split("").union(x.ban_url.split("")).distinct.mkString("", ", ", "")))
//    val df: DataFrame = value.toDF()

//    df.coalesce(1)
//      .write.mode(SaveMode.Overwrite)
//      .format("com.databricks.spark.csv")
//      .save("C:\\Users\\31343\\Desktop\\cos_dict_result")
//    val frame: DataFrame = df.withColumn("id", monotonically_increasing_id)
//    frame.select("ban_url_union").collect()

  }


  case class CosDict(tdbankImpDate:String,domain:String,url:String,banUrl:String)
}
