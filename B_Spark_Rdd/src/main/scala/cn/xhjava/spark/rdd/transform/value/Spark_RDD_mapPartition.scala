package cn.xhjava.spark.rdd.transform.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 类似于map，但独立地在RDD的每一个分片上运行，因此在类型为T的RDD上运行时，
  * func的函数类型必须是Iterator[T] => Iterator[U]。
  * 假设有N个元素，有M个分区，那么map的函数的将被调用N次,
  * 而mapPartitions被调用M次,一个函数一次处理所有分区。
  */
object Spark_RDD_mapPartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_RDD_mapPartition")
    val sc = new SparkContext(conf)

    val listRdd: RDD[Int] = sc.makeRDD(1 to 10)

    // mapPartition可以对RDD中所有的分区进行遍历
    // mapPartition效率优先map算子,减少了执行器执行交互次数
    // mapPartition可能出现内存溢出(OOM)
    val partitionRdd: RDD[Int] = listRdd.mapPartitions(datas => {
      datas.map(_ * 2)
    })
    //收集并且打印
    partitionRdd.collect().foreach(println)
  }
}
