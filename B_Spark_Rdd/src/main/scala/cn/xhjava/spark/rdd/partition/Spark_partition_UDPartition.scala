package cn.xhjava.spark.rdd.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * 自定义分区
  */
object Spark_RDD_partitionBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_RDD_partitionBy")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "c")), 3)
    //分区
    val partitionRDD: RDD[(Int, String)] = rdd.partitionBy(new MyPartitioner(2))

    println(partitionRDD.partitions)
  }
}


/**
  * 自定义分区器
  * 继承Partitioner
  */
class MyPartitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = {
    partitions
  }

  //返回分区索引,将所有数据存入分区1
  override def getPartition(key: Any): Int = {
    1
  }
}
