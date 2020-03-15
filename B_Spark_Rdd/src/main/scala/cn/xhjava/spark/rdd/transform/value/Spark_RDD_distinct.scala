package cn.xhjava.spark.rdd.transform.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 对源RDD进行去重后返回一个新的RDD。
  * 默认情况下，只有8个并行任务来操作，但是可以传入一个可选的numTasks参数改变它。
  */
object Spark_RDD_distinct {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_RDD_distinct")
    val sc = new SparkContext(conf)

    val listRdd: RDD[Int] = sc.makeRDD(Array(1, 3, 2, 5, 6, 1, 6, 5, 2))


    // val distinctRdd: RDD[Int] = listRdd.distinct()
    //使用distinct算子对数据进行去重,但是因为去重后会导致数据减少,所有可以改变默认的分区数量
    // 该算子存在一个shuffle的行为
    val distinctRdd: RDD[Int] = listRdd.distinct(3)

    //收集并且打印
    distinctRdd.collect().foreach(println)
  }
}
