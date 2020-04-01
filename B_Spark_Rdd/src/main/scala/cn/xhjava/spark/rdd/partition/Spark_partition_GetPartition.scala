package cn.xhjava.spark.rdd.partition

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD


/**
  * 获取partition类型
  */
object Spark_partition_GetPartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_Dependen_Demo")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List((1, 1), (2, 2), (3, 3)))

    //查看RDD分区器
    rdd.partitions.foreach(println)
    //    org.apache.spark.rdd.ParallelCollectionPartition@691
    //    org.apache.spark.rdd.ParallelCollectionPartition@692
    //    org.apache.spark.rdd.ParallelCollectionPartition@693

    //使用HashPartitioner对RDD进行重新分区
    val rdd2: RDD[(Int, Int)] = rdd.partitionBy(new HashPartitioner(2))
    //再次查看分区器
    rdd2.partitions.foreach(println)
    //org.apache.spark.rdd.ShuffledRDDPartition@0
    //org.apache.spark.rdd.ShuffledRDDPartition@1
  }
}
