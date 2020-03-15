package cn.xhjava.spark.rdd.transform.value_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 对源RDD和参数RDD求交集后返回一个新的RDD
  * 求两个RDD的交集
  */
object Spark_RDD_intersection {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_RDD_intersection")
    val sc = new SparkContext(conf)

    //创建第一个RDD
    val listRdd: RDD[Int] = sc.makeRDD(1 to 10)
    //创建第二个RDD
    val listRdd2: RDD[Int] = sc.makeRDD(5 to 15)

    val intersectionRDD: RDD[Int] = listRdd.intersection(listRdd2)


    //收集并且打印
    intersectionRDD.collect().foreach(println)
  }
}
