package cn.xhjava.spark.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将数据集的元素以textfile的形式保存到HDFS文件系统或者其他支持的文件系统，
  * 对于每个元素，Spark将会调用toString方法，将它装换为文件中的文本
  */
object Spark_Action_saveAsTextFile {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_Action_saveAsTextFile")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[Int] = sc.makeRDD(1 to 10, 2)

    rdd.saveAsTextFile("outpath")
    //打印结果:

  }
}
