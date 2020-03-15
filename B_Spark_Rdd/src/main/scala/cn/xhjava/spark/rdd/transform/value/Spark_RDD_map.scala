package cn.xhjava.spark.rdd.transform.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 返回一个新的RDD，该RDD由每一个输入元素经过func函数转换后组成
  */
object Spark_RDD_map {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_RDD_map")
    val sc = new SparkContext(conf)

    val listRdd: RDD[Int] = sc.makeRDD(1 to 10)


    val partitionRdd: RDD[Int] = listRdd.map((_ * 2))

    //收集并且打印
    partitionRdd.collect().foreach(println)
  }
}
