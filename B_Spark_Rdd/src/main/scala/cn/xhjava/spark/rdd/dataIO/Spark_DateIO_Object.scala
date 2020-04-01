package cn.xhjava.spark.rdd.dataIO

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 对象文件
  * 对象文件是将对象序列化后保存的文件，采用Java的序列化机制。
  * 可以通过objectFile[k,v](path) 函数接收一个路径，读取对象文件，返回对应的 RDD，
  * 也可以通过调用saveAsObjectFile() 实现对对象文件的输出。因为是序列化所以要指定类型
  */
object Spark_DateIO_Object {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_Dependen_Demo")
    val sc = new SparkContext(conf)

    //
    val rdd: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4))

    //将RDD保存为Object文件
    rdd.saveAsObjectFile("in/object")
    sc.objectFile[Int]("in/object").collect().foreach(println)
  }
}
