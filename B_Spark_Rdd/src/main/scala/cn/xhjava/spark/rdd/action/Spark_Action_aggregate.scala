package cn.xhjava.spark.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * aggregate函数将每个分区里面的元素通过seqOp和初始值进行聚合，
  * 然后用combine函数将每个分区的结果和初始值(zeroValue)进行combine操作。
  * 这个函数最终返回的类型不需要和RDD中元素类型一致。
  *
  * 与aggregateByKey类似
  */
object Spark_Action_aggregate {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_Action_aggregate")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[Int] = sc.makeRDD(1 to 10, 2)

    println(rdd.aggregate(0)(_ + _, _ + _))
    //打印结果:55

  }
}
