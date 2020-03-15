package cn.xhjava.spark.rdd.transform.value_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 计算差的一种函数，去除两个RDD中相同的元素，不同的RDD将保留下来
  * 求两个RDD的差集
  */
object Spark_RDD_subtract {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_RDD_subtract")
    val sc = new SparkContext(conf)

    //创建第一个RDD
    val listRdd: RDD[Int] = sc.makeRDD(1 to 10)
    //创建第二个RDD
    val listRdd2: RDD[Int] = sc.makeRDD(10 to 15)

    val subtractRDD: RDD[Int] = listRdd.subtract(listRdd2)

    //收集并且打印
    subtractRDD.collect().foreach(println)
  }
}
