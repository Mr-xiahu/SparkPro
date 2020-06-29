package cn.xhjava.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  * @author Xiahu
  * @create 2020/6/29
  *
  *         转换
  */
object H_Transform {
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
    //todo Driver 端 执行1次
    rid.map {
      case x => {
        //todo Executor 端 执行n次 (n表示Excutor的个数)
        x
      }
    }

    //转换
    //todo Driver 端 执行1次
    rid.transform {
      case rdd => {
        //todo Driver 端执行 执行m 次, m表示采集周期
        rdd.map{
          case x => {
            //TODO Executor 端  执行n次 (n表示Excutor的个数)
            x
          }
        }
      }
    }


    //解析
    val dstream: DStream[String] = rid.flatMap(_.split(" "))
    val dstream2: DStream[(String, Int)] = dstream.map((_, 1))






    //启动SparkStreaming最重要的设置
    //1.启动采集器
    ssc.start()
    //2.driver等待采集器执行
    ssc.awaitTermination()
  }
}
