package cn.xhjava.spark.stream

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * 自定义采集器
  */
object C_MyReceiver {
  def main(args: Array[String]): Unit = {
    //1.初始化SparkConf
    val conf: SparkConf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("Spark Streaming Demo")
    //2.实时数据分析环境对象
    //采集周期：以指定的时间为周期采集实时数据
    val ssc = new StreamingContext(conf, Seconds(5))
    //远程监控端口
    val rid: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver("118.89.20.151", 9999))


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

/**
  * 声明自定义采集器,继承Receiver类，实现抽象方法
  */
class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  var socket: Socket = null


  def receive(): Unit = {
    socket = new Socket(host, port)
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))
    var line: String = null
    while ((line = reader.readLine()) != null) {
      //将采集的数据存储到采集器的内部进行转换
      if ("END".equals(line)) {
        return
      } else {
        this.store(line)
      }
    }
  }


  override def onStart(): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        receive()
      }
    })
  }

  override def onStop(): Unit = {
    socket.close()
    socket = null
  }
}
