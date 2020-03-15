package cn.xhjava.spark.rdd.transform.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * 对pairRDD进行分区操作，如果原有的partionRDD和现有的partionRDD是一致的话就不进行分区，
  * 否则会生成ShuffleRDD，即会产生shuffle过程。
  */
object Spark_RDD_partitionBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_RDD_partitionBy")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "c")), 3)
    //分区
    val partitionRDD: RDD[(Int, String)] = rdd.partitionBy(new MyPartitioner(2))
    partitionRDD.saveAsTextFile("output")
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
