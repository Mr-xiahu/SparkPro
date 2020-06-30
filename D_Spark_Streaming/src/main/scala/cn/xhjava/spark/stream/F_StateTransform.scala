package cn.xhjava.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

/**
  *
  * @author Xiahu
  * @create 2020/6/29
  *
  *         状态转换
  */
object F_StateTransform {
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

    //todo 保存数据的状态,需要设定检查点的路径
    ssc.checkpoint("/tmp")

    //解析
    val dstream: DStream[String] = rid.flatMap(_.split(" "))
    val dstream2: DStream[(String, Int)] = dstream.map((_, 1))

    //将转换结构后的数据做聚合操作
    val value: DStream[(String, Int)] = dstream2.updateStateByKey {
      case (seq, buffer) => {
        val sum: Int = buffer.getOrElse(0) + seq.sum
        Option(sum)
      }
    }
    println(value)

    /**
      * 无状态数据与有状态数据:
      *  无状态:       有状态:
      *   (A,1)        (A,1)
      *   (A,1)        (A,1),(A,1)
      *   (A,3)        (A,3),(A,1),(A,1)
      *   (A,1)        (A,1),(A,3),(A,1),(A,1)
      *   (A,2)        (A,2),(A,2),(A,1),(A,3),(A,1),(A,1)
      *
      *   有状态的数据每次都会带着之前的数据一起返回
      */





    //启动SparkStreaming最重要的设置
    //1.启动采集器
    ssc.start()
    //2.driver等待采集器执行
    ssc.awaitTermination()
  }
}
