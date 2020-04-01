package cn.xhjava.spark.rdd.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}


/**
  * HashPartition
  */
object Spark_partition_HashPartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_Dependen_Demo")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List((1, 1), (2, 2), (3, 3)))

    rdd.mapPartitionsWithIndex((index, iter) => {
      Iterator(index.toString + " : " + iter.mkString("|"))
    }).collect.foreach(println)

    //查看当前分区器
    println(rdd.partitions)

    //设置新的分区器
    val rdd2: RDD[(Int, Int)] = rdd.partitionBy(new HashPartitioner(8))
    //查看新设置后的分区器
    println(rdd2.partitions)
  }
}
