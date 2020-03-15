package cn.xhjava.spark.rdd.transform.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 过滤。
  * 返回一个新的RDD，该RDD由经过func函数计算后返回值为true的输入元素组成。
  */
object Spark_RDD_filter {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_RDD_filter")
    val sc = new SparkContext(conf)

    val listRdd: RDD[Int] = sc.makeRDD(1 to 10)


    val filterRdd: RDD[Int] = listRdd.filter(_ % 2 == 0)

    //收集并且打印
    filterRdd.collect().foreach(println)
  }
}
