package cn.xhjava.spark.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 折叠操作，aggregate的简化操作，seqop和combop一样
  */
object Spark_Action_fold {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_Action_fold")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[Int] = sc.makeRDD(1 to 10, 2)

    println(rdd.fold(0)(_ + _))
    //打印结果:55


  }
}
