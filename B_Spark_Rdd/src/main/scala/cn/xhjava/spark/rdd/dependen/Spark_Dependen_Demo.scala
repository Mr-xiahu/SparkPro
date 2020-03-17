package cn.xhjava.spark.rdd.dependen

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 查看血缘关系所需要的方法
  */
object Spark_Dependen_Demo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_Dependen_Demo")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("a", "b", "c", "d", "a"))


    val mapRDD: RDD[(String, Int)] = rdd.map((_, 1))

    val reduceByKeyRDD: RDD[(String, Int)] = mapRDD.reduceByKey((_ + _))

    //查看血缘
    println(reduceByKeyRDD.toDebugString)
    //(3) ShuffledRDD[2] at reduceByKey at Spark_Dependen_Demo.scala:20 []
    //  +-(3) MapPartitionsRDD[1] at map at Spark_Dependen_Demo.scala:18 []
    //    |  ParallelCollectionRDD[0] at makeRDD at Spark_Dependen_Demo.scala:15 []

    //查看依赖类型
    println(reduceByKeyRDD.dependencies)
    //List(org.apache.spark.ShuffleDependency@10ef5fa0)

    //收集并且打印
    reduceByKeyRDD.collect().foreach(println)
  }
}
