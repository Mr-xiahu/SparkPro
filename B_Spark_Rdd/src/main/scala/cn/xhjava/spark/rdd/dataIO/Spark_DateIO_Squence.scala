package cn.xhjava.spark.rdd.dataIO

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * SequenceFile文件写入
  * SequenceFile文件是Hadoop用来存储二进制形式的key-value对而设计的一种平面文件(Flat File)
  */
object Spark_DateIO_Squence {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_Dependen_Demo")
    val sc = new SparkContext(conf)

    //生成RDD
    val rdd: RDD[(Int, Int)] = sc.makeRDD(Array((1,2),(3,4),(5,6)))

    //将读取的数据保存到Sequence文本
    rdd.saveAsObjectFile("in/Sequence")
    //读取Sequence 文件打印
    sc.sequenceFile[Int,Int]("in/Sequence").collect().foreach(println)
  }
}
