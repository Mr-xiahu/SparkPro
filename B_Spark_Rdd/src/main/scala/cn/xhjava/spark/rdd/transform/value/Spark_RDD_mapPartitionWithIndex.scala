package cn.xhjava.spark.rdd.transform.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 类似于mapPartitions，但func带有一个整数参数表示分片的索引值，
  * 因此在类型为T的RDD上运行时，func的函数类型必须是(Int, Interator[T]) => Iterator[U]；
  */
object Spark_RDD_mapPartitionWithIndex {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_RDD_mapPartitionWithIndex")
    val sc = new SparkContext(conf)
    //1.创建数据,5分区
    val mapRdd: RDD[Int] = sc.makeRDD(1 to 10,5)
    //mapPartitionWithIndex
    val tupleRdd: RDD[(Int, String)] = mapRdd.mapPartitionsWithIndex {
      case (partitionNum, datas) => {
        datas.map((_, "分区:" + partitionNum))
      }
    }

    //收集并且打印
    tupleRdd.collect().foreach(println)
  }
}
