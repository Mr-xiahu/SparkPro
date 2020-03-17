package cn.xhjava.spark.rdd.funcationtransmit

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 在实际开发中我们往往需要自己定义一些对于RDD的操作，
  * 那么此时需要主要的是，初始化工作是在Driver端进行的，而实际运行程序是在Executor端进行的，
  * 这就涉及到了跨进程通信，是需要序列化的。
  */
object MethodTransmit {
  def main(args: Array[String]): Unit = {
    //1.初始化配置信息及SparkContext
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    //2.创建一个RDD
    val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "atguigu"))

    //3.创建一个Search对象
    val search = new Search("h")

    //4.运用第一个过滤函数并打印结果
    val match1: RDD[String] = search.getMatch1(rdd)

    /**
      * 报错: org.apache.spark.SparkException: Task not serializable
      * 问题说明: 请看class Search
      */


    match1.collect().foreach(println)
  }

}

class Search(query: String) extends  Serializable {
  //过滤出包含字符串的数据
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  //过滤出包含字符串的RDD
  def getMatch1(rdd: RDD[String]): RDD[String] = {
    rdd.filter(isMatch)
  }

  /**
    * 问题: 在这一页代码中,那些逻辑是运行在Driver,哪些逻辑是运行在Exector端呢?
    *  首先我们得明白,RDD算子都是运行在Exector端的,所以从一开始:
    *     val search = new Search("h")
    *     search.getMatch1(rdd)
    *   这些方法是运行在Driver的,等到了:
    *     rdd.filter(isMatch)
    *   filter(xxx)内传入的是一个方法,是什么方法呢?
    *     def isMatch(s: String): Boolean = {
    *         s.contains(query)
    *     }
    *   相当于执行了this.isMatch(),但是在Exector端,由于没有实现序列化,谁知道你的this是谁呢
    */

  //解决方法:
  // 继承Serializable方法




  //过滤出包含字符串的RDD
  def getMatche2(rdd: RDD[String]): RDD[String] = {
    rdd.filter(x => x.contains(query))
  }


}

