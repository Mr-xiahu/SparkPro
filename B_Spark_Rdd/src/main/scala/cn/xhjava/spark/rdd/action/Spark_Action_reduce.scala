package cn.xhjava.spark.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 通过func函数聚集RDD中的所有元素，先聚合分区内数据，再聚合分区间数据
  */
object Spark_Action_reduce {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_Action_reduce")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[Int] = sc.makeRDD(1 to 10, 2)
    //创建RDD2
    val rdd2: RDD[(String, Int)] = sc.makeRDD(Array(("a", 1), ("a", 3), ("c", 3), ("d", 5)))

    //聚合rdd内的所有元素
    val reduceRDD: Int = rdd.reduce(_ + _)
    println(reduceRDD)
    //打印结果:55
    //聚合rdd2[String]所有数据
    val reduceRDD2: (String, Int) = rdd2.reduce((x,y) =>(x._1+y._1,x._2+y._2))
    println(reduceRDD2)
    //打印结果:(acda,12)

  }
}
