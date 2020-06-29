package cn.xhjava.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  * @author Xiahu
  * @create 2020/6/29
  *
  *         spark 内使用窗口
  */
object G_WindowDemo2 {
  def main(args: Array[String]): Unit = {
    //1.初始化SparkConf
    val conf: SparkConf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("Spark Streaming Demo")
    //2.实时数据分析环境对象
    //采集周期：以指定的时间为周期采集实时数据
    val ssc = new StreamingContext(conf, Seconds(5))
    //远程监控端口
    val rid: DStream[String] = ssc.textFileStream("in/sparkstream/test")

    //窗口大小应该为采集周期的整数倍,窗口滑动的步长也是采集周期的整数倍
    val windowDstream: DStream[String] = rid.window(Seconds(10), Seconds(10))


    //解析
    val dstream: DStream[String] = windowDstream.flatMap(_.split(" "))
    val dstream2: DStream[(String, Int)] = dstream.map((_, 1)).reduceByKey((_ + _))
    println(dstream2)





    //启动SparkStreaming最重要的设置
    //1.启动采集器
    ssc.start()
    //2.driver等待采集器执行
    ssc.awaitTermination()
  }
}
