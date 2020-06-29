package cn.xhjava.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  * @author Xiahu
  * @create 2020/6/29
  *         scala窗口操作
  */
object G_WindowDemo {
  def main(args: Array[String]): Unit = {
    val list = List(1, 2, 3, 4, 5, 6, 7, 8)
    //滑动窗口
    val iterator: Iterator[List[Int]] = list.sliding(3)
    for (num <- iterator) {
      println(num)
    }
  }
}
