package cn.xhjava.spark.rdd.transform.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 类似于map，但是每一个输入元素可以被映射为0或多个输出元素
  * （所以func应该返回一个序列，而不是单一元素）
  */
object Spark_RDD_flatMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_RDD_flatMap")
    val sc = new SparkContext(conf)

    val listRdd: RDD[List[Int]] = sc.makeRDD(Array(List(1, 2), List(3, 4)))

    val flatMapRdd: RDD[Int] = listRdd.flatMap(datas => datas)
    flatMapRdd.collect().foreach(println)
  }
}
