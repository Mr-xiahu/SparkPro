package cn.xhjava.spark.rdd.dataIO

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.parsing.json.JSON

/**
  * Json文件的读取
  * 如果JSON文件中每一行就是一个JSON记录，那么可以通过将JSON文件当做文本文件来读取，然后利用相关的JSON库对每一条数据进行JSON解析。
  *
  */
object Spark_DateIO_Json {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_DateIO_Json")
    val sc = new SparkContext(conf)

    //TODO 注意：使用RDD读取JSON文件处理很复杂，同时SparkSQL集成了很好的处理JSON文件的方式，所以应用中多是采用SparkSQL处理JSON文件


    //读取json文本
    val jsonRDD: RDD[String] = sc.textFile("F:\\MyLearning\\SparkStudy\\in\\people.json")

    //解析json数据并且打印
    jsonRDD.map(JSON.parseFull).collect().foreach(println)
  }
}
