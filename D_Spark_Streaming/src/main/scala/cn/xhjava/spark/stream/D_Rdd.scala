package cn.xhjava.spark.stream

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * 循环创建几个RDD，将RDD放入队列。通过SparkStream创建Dstream，计算WordCount
  */
object D_Rdd {
  def main(args: Array[String]): Unit = {
    //1.初始化SparkConf
    val conf: SparkConf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("Spark Streaming Demo")
    //2.实时数据分析环境对象
    //采集周期：以指定的时间为周期采集实时数据
    val ssc = new StreamingContext(conf, Seconds(5))
    //3.创建RDD队列
    val rddQueue = new mutable.Queue[RDD[Int]]()

    //4.创建QueueInputDStream
    val inputStream = ssc.queueStream(rddQueue, oneAtATime = false)

    //5.处理队列中的RDD数据
    val mappedStream = inputStream.map((_, 1))
    val reducedStream = mappedStream.reduceByKey(_ + _)

    //6.打印结果
    reducedStream.print()

    //7.启动任务
    ssc.start()

    //8.循环创建并向RDD队列中放入RDD
    for (i <- 1 to 5) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
      Thread.sleep(2000)
    }

    //启动SparkStreaming最重要的设置
    //9.driver等待采集器执行
    ssc.awaitTermination()

  }
}
