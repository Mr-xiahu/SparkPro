package cn.xhjava.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Spark Streaming WordCount
  */
object A_HelloWorld {
  def main(args: Array[String]): Unit = {
    //1.初始化SparkConf
    val conf: SparkConf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("Spark Streaming Demo")
    //2.实时数据分析环境对象
    //采集周期：以指定的时间为周期采集实时数据
    val ssc = new StreamingContext(conf, Seconds(5))
    //远程监控端口  nc -lk 9998
    val rid: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.0.112", 9998)
    //解析
    val dstream: DStream[String] = rid.flatMap(_.split(" "))
    val dstream2: DStream[(String, Int)] = dstream.map((_, 1)).reduceByKey(_ + _)
    dstream2.print()

    //启动SparkStreaming最重要的设置
    //1.启动采集器
    ssc.start()
    //2.driver等待采集器执行
    ssc.awaitTermination()

  }
}
