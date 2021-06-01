package com.zhenyu.sparkwordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession, types}
import org.apache.spark.sql.catalyst.dsl.expressions.longToLiteral
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.Map

object SparkWordCount {
  def main(args: Array[String]): Unit = {
//    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkWordCount")
//    val sc = new SparkContext(conf)
//    val line: RDD[(String, Int)] = sc.textFile("D:\\data\\input\\demo1")
//      .flatMap(_.split(","))
//      .map((_, 1))
//      .reduceByKey(_ + _)
//    println(line.collect().mkString("\n"))
//  val spark: SparkSession = new SparkSession
//    .Builder()
//    .master("local[*]")
//    .appName("SparkWordCount")
//    .getOrCreate()
//    val lines: RDD[String] = spark.sparkContext.textFile("D:\\data\\input\\demo1")
//    val line: RDD[(String, Int)] = lines.flatMap(_.split(","))
//      .map((_, 1))
//      .reduceByKey(_ + _)
//    println(line.collect().mkString("\n"))

    val spark: SparkSession = new SparkSession.Builder()
      .master("local[*]")
      .appName("DataFrameTest")
      .getOrCreate()
    val lines: RDD[String] = spark.sparkContext.textFile("D:\\data\\input\\strong_uin")
//    val lines: RDD[String] = spark.sparkContext.textFile("/tmp/data/random_uin")
//    val line: RDD[StrongUin] = lines
//      .map(_.split(","))
//      .map(x => {
////        println(x.toString)
//        StrongUin(x(0).toInt, x(1).toInt, x(2).toInt)
//      })
//    import spark.implicits._
//    val df: DataFrame = line.toDF()
//    df.printSchema()
//    df.show()


    //自定义schema
//    val schemaString = "ftime,id,uin"
//    val field: Array[StructField] = schemaString.split(",").map(x => {
//      case "ftime" => StructField("ftime", IntegerType, nullable = true)
//      case "id" => StructField("id", IntegerType, nullable = true)
//      case _ => StructField("uin", IntegerType, nullable = true)
//    })
//  val fields: Array[StructField] = schemaString.split(",").map(x => {
//    StructField(x, StringType, nullable = true)
//})
//    val schema: types.StructType = StructType(fields)
//    import spark.implicits._
//    val line: RDD[Row] = lines.map(_.split(",")).map(x => {
//      Row(x(0), x(1), x(2))
//    })
//    val df: DataFrame = spark.createDataFrame(line, schema)
//    df.printSchema()
//    df.show()
//    df.select("id").show()
//    df.select($"ftime",$"uin").groupBy("ftime","uin").count().show()
//val schema: StructType = StructType(List(
//  StructField("ftime", StringType, nullable = true),
//  StructField("id", StringType, nullable = true),
//  StructField("uin", StringType, nullable = true)
//))
//    val line: RDD[Row] = lines.map(_.split(",")).map(x => {
//      Row(x(0), x(1), x(2))
//    })
//
//    val df: DataFrame = spark.createDataFrame(line, schema)
//    df.show()
val line: Array[StrongUin] = lines.map(_.split(",")).map(x => StrongUin(x(1), x(2))).collect()
    println(line.length)
    val list = List()
    var data:mutable.Map[String,String] = mutable.Map()
    line.foreach(x=>{
      data += (x.uin -> x.uin_strong)
    })
    val li: List[mutable.Map[String, String]] = list ::: List(data)

    println(li)

  }

  case class StrongUin(uin:String,uin_strong:String)

}
