package cn.xhjava.spark.rdd.transform.value_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 笛卡尔积（尽量避免使用）
  * 创建两个RDD，计算两个RDD的笛卡尔积
  */
object Spark_RDD_cartesian {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_RDD_cartesian")
    val sc = new SparkContext(conf)

    //创建第一个RDD
    val listRdd: RDD[Int] = sc.makeRDD(1 to 3)
    //创建第二个RDD
    val listRdd2: RDD[Int] = sc.makeRDD(2 to 6)

    val cartesianRDD: RDD[(Int, Int)] = listRdd.cartesian(listRdd2)


    //收集并且打印
    cartesianRDD.collect().foreach(println)
  }
}
