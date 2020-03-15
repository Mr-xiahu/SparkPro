package cn.xhjava.spark.rdd.transform.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 管道，针对每个分区，都执行一个shell脚本，返回输出的RDD
  * 注意：脚本需要放在Worker节点可以访问到的位置
  */
object Spark_RDD_pipe {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_RDD_pipe")
    val sc = new SparkContext(conf)

    val listRdd: RDD[String] = sc.parallelize(List("hi", "Hello", "how", "are", "you"), 2)

    //需要在Linux机器上测试
    val pipeRDD: RDD[String] = listRdd.pipe("E:\\Tmp\\Spark_pipe_test.sh")

    //收集并且打印
    pipeRDD.collect().foreach(println)
  }
}
