package cn.xhjava.spark.rdd.cache

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * RDD通过persist方法或cache方法可以将前面的计算结果缓存，
  * 默认情况下 persist() 会把数据以序列化的形式缓存在 JVM 的堆空间中。
  * 但是并不是这两个方法被调用时立即缓存，而是触发后面的action时，
  * 该RDD将会被缓存在计算节点的内存中，并供后面重用。
  */
object Spark_RDD_cache_Demo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_RDD_cache_Demo")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("xiahu", "tom", "jeck"))
    //将rdd 通过map 转换为tuple
    val mapRDD: RDD[(String, Long)] = rdd.map((_, System.currentTimeMillis()))

    //多次打印结果
    mapRDD.collect().foreach(println)
    Thread.sleep(5000)
    mapRDD.collect().foreach(println)
    //此次打印的时间戳是不一致的




    /**
      * 缓存有可能丢失，或者存储于内存的数据由于内存不足而被删除，
      * RDD的缓存容错机制保证了即使缓存丢失也能保证计算的正确执行。
      * 通过基于RDD的一系列转换，丢失的数据会被重算，由于RDD的各个Partition是相对独立的，因此只需要计算丢失的部分即可，并不需要重算全部Partition。
      */

    //缓存当前mapRDD
    mapRDD.cache()
    //多次打印结果
    mapRDD.collect().foreach(println)
    Thread.sleep(5000)
    mapRDD.collect().foreach(println)

  }
}
