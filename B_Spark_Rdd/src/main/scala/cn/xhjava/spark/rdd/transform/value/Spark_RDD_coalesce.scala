package cn.xhjava.spark.rdd.transform.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 缩减分区数，用于大数据集过滤后，提高小数据集的执行效率。
  */
object Spark_RDD_coalesce {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_RDD_coalesce")
    val sc = new SparkContext(conf)
    //创建一个4分区的RDD
    val listRdd: RDD[Int] = sc.makeRDD(1 to 10, 4)
    println("缩减分区前的分区数量:" + listRdd.partitions.size)

    val coalesceRdd: RDD[Int] = listRdd.coalesce(2)
    println("缩减分区后的分区数量:" + coalesceRdd.partitions.size)

    //收集并且打印
    coalesceRdd.collect().foreach(println)
  }
}
