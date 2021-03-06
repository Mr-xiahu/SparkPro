package cn.xhjava.spark.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 返回RDD中元素的个数
  */
object Spark_Action_count {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_Action_count")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[Int] = sc.makeRDD(1 to 10, 2)

    println(rdd.count())
    //打印结果:10

  }
}
