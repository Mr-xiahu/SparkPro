package cn.xhjava.spark.rdd.transform.value_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将两个RDD组合成Key/Value形式的RDD
  * 这里默认两个RDD的partition数量以及元素数量都相同，否则会抛出异常。
  * 创建两个RDD，并将两个RDD组合到一起形成一个(k,v)RDD
  */
object Spark_RDD_zip {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_RDD_zip")
    val sc = new SparkContext(conf)

    //创建第一个RDD
    val listRdd: RDD[Int] = sc.makeRDD(1 to 5,5)
    //创建第二个RDD
    val listRdd2: RDD[String] = sc.makeRDD(List("hello","word","my","name","xiahu"),5)

    val zipRDD: RDD[(Int, String)] = listRdd.zip(listRdd2)

    //val listRdd: RDD[Int] = sc.makeRDD(1 to 5,5)
    //val listRdd2: RDD[String] = sc.makeRDD(List("hello","word","my","name"),4)
    //上述情况会报错


    //收集并且打印
    zipRDD.collect().foreach(println)
  }
}
