package cn.xhjava.spark.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将数据集中的元素以Hadoop sequencefile的格式保存到指定的目录下，
  * 可以使HDFS或者其他Hadoop支持的文件系统
  */
object Spark_Action_saveAsSequenceFile {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_Action_saveAsSequenceFile")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[Int] = sc.makeRDD(1 to 10, 2)


    //打印结果:

  }
}
