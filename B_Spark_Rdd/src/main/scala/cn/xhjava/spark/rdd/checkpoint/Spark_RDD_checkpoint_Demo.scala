package cn.xhjava.spark.rdd.checkpoint

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark_RDD_checkpoint_Demo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_RDD_checkpoint_Demo")
    val sc = new SparkContext(conf)
    //设置checkpoint 位置
    sc.setCheckpointDir("checkpoint")

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //将rdd 通过map 转换为tuple
    val mapRDD: RDD[(Int, Long)] = rdd.map((_, 1))


    val result: RDD[(Int, Long)] = mapRDD.reduceByKey(_ + _)

    //设置checkPoint
    result.checkpoint()


    result.collect().foreach(println)

    //查看血缘
    println(result.toDebugString)
    //(3) ShuffledRDD[2] at reduceByKey at Spark_RDD_checkpoint_Demo.scala:20 []
    //|  ReliableCheckpointRDD[3] at collect at Spark_RDD_checkpoint_Demo.scala:26 []
    //该血缘直接与reduceByKey算子后的shuffer关联,不会与map算子的血缘关联,因为是从checkpoint文件夹获取的数据信息
  }
}
