package cn.xhjava.spark.rdd.transform.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_RDD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_RDD")
    val sc = new SparkContext(conf)

    //RDD创建的两种方式:parallelize和makeRDD
    //1.从内存中parallelize
    val rddOne: RDD[Int] = sc.parallelize(Array(1,2,3,4,5,6,7,8))
    //2.从内存中makeRDD(底层也是勇敢parallelize实现)
    val rddTwo: RDD[Int] = sc.makeRDD(Array(1,2,3,4,5,6,7,8))

    //自定义分区数
    //通过源码可见,defaultParallelism 默认为local[N]内的N.
    val rddTwo2: RDD[Int] = sc.makeRDD(Array(1,2,3,4,5,6,7,8),2)

    //3.从外部存储中创建
    //val rddThree: RDD[String] = sc.textFile("")
    //读取文件时,传递的分区参数为最小分区数量,但是不一定时这个分区数,取决于hadoop读取文件时分片规则
    //val rddThree: RDD[String] = sc.textFile("path",2)最终的分区数量不一定是2


    rddOne.saveAsTextFile("output")
  }

}
