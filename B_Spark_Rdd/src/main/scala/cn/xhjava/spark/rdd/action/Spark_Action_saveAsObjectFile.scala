package cn.xhjava.spark.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 用于将RDD中的元素序列化成对象，存储到文件中
  */
object Spark_Action_saveAsObjectFile {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_Action_saveAsObjectFile")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[Int] = sc.makeRDD(1 to 10, 2)

    rdd.saveAsObjectFile("outpath")
    //打印结果:

  }
}
