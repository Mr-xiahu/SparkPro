package cn.xhjava.spark.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 针对(K,V)类型的RDD，返回一个(K,Int)的map，表示每一个key对应的元素个数
  */
object Spark_Action_countByKey {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_Action_countByKey")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[(Int, Int)] = sc.makeRDD(List((1, 3), (1, 2), (1, 4), (2, 3), (3, 6), (3, 8)), 3)

    rdd.collect().foreach(println)
    //打印结果:
    //    (1,3)
    //    (1,2)
    //    (1,4)
    //    (2,3)
    //    (3,6)
    //    (3,8)

  }
}
