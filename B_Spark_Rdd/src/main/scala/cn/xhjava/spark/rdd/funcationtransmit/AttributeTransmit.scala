package cn.xhjava.spark.rdd.funcationtransmit

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 在实际开发中我们往往需要自己定义一些对于RDD的操作，
  * 那么此时需要主要的是，初始化工作是在Driver端进行的，而实际运行程序是在Executor端进行的，
  * 这就涉及到了跨进程通信，是需要序列化的。
  */
object AttributeTransmit {
  def main(args: Array[String]): Unit = {
    //1.初始化配置信息及SparkContext
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    //2.创建一个RDD
    val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "atguigu"))

    //3.创建一个Search对象
    val search = new Search2("h")

    //4.运用第一个过滤函数并打印结果
    val match1: RDD[String] = search.getMatche2(rdd)
    match1.collect().foreach(println)

  }

}

class Search2(query: String){
  //过滤出包含字符串的数据
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  //过滤出包含字符串的RDD
  def getMatch1(rdd: RDD[String]): RDD[String] = {
    rdd.filter(isMatch)
  }



  //过滤出包含字符串的RDD
  def getMatche2(rdd: RDD[String]): RDD[String] = {
    rdd.filter(x => x.contains(query))
  }

  //在这个方法中所调用的变量：query 是定义在Search这个类中的字段，
  //实际上调用的是this.query，this表示Search这个类的对象，
  //程序在运行过程中需要将Search对象序列化以后传递到Executor端。

  //解决方法:
  //实现序列化接口

}

