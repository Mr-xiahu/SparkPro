package cn.xhjava.spark.rdd.transform.value_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 对源RDD和参数RDD求并集后返回一个新的RDD
  * 求两个RDD的并集
  */
object Spark_RDD_union {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_RDD_union")
    val sc = new SparkContext(conf)

    //创建第一个RDD
    val listRdd: RDD[Int] = sc.makeRDD(1 to 5)
    //创建第二个RDD
    val listRdd2: RDD[Int] = sc.makeRDD(10 to 15)

    val unionRDD: RDD[Int] = listRdd.union(listRdd2)


    //收集并且打印
    unionRDD.collect().foreach(println)
  }
}
