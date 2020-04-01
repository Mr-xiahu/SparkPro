package cn.xhjava.spark.rdd.dataIO

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Text文件的读取与写入
  */
object Spark_DateIO_Text {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_DateIO_Text")
    val sc = new SparkContext(conf)

    //读取txt文本
    val textRDD: RDD[String] = sc.textFile("F:\\MyLearning\\SparkStudy\\in\\test")

    //将读取的数据保存到txt文本
    textRDD.saveAsTextFile("in/test2")
  }
}
