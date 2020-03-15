package cn.xhjava.spark.rdd.transform.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 根据分区数，重新通过网络随机洗牌所有数据。
  */
object Spark_RDD_repartition {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_RDD_repartition")
    val sc = new SparkContext(conf)

    //创建4个分区的RDD
    val listRdd: RDD[Int] = sc.makeRDD(1 to 16, 4)
    println("重新分区前分区数:" + listRdd.partitions.size)
    val repartitionRDD: RDD[Int] = listRdd.repartition(2)
    println("重新分区前分区数:" + repartitionRDD.partitions.size)
    //收集并且打印
    repartitionRDD.collect().foreach(println)

    // coalesce和repartition的区别
    // coalesce重新分区，可以选择是否进行shuffle过程
    // repartition:通过源码发现,强制进行shuffle.
  }
}
