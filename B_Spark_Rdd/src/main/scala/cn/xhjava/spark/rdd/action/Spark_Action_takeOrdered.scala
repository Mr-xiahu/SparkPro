package cn.xhjava.spark.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 返回该RDD排序后的前n个元素组成的数组
  */
object Spark_Action_takeOrdered {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_Action_takeOrdered")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[Int] = sc.makeRDD(1 to 10, 2)

    rdd.takeOrdered(4).foreach(println)
    //打印结果:
    //1
    //2
    //3
    //4

  }
}
