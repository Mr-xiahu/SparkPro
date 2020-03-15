package cn.xhjava.spark.rdd.transform.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *将每一个分区形成一个数组，形成新的RDD类型时RDD[Array[T]
  */
object Spark_RDD_glom {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_RDD_glom")
    val sc = new SparkContext(conf)

    val listRdd: RDD[Int] = sc.makeRDD(1 to 16, 4)


    //将一个分区中的数据放入一个数组中
    val glomRDD: RDD[Array[Int]] = listRdd.glom()

    //收集并且打印
    glomRDD.collect().foreach(array => {
      println(array.mkString(","))
    })
  }
}
