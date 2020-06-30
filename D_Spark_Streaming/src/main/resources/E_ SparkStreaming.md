## E. SparkStreaming

#### 一.SparkStreaming概述

##### 1. SparkStreaming是什么?

Spark Streaming用于流式数据的处理。Spark Streaming支持的数据输入源很多，例如：Kafka、Flume、Twitter、ZeroMQ和简单的TCP套接字等等。

数据输入后可以用Spark的高度抽象原语如：map、reduce、join、window等进行运算。而结果也能保存在很多地方，如HDFS，数据库等。

![](http://spark.apache.org/docs/latest/img/streaming-arch.png)

和Spark基于RDD的概念很相似，Spark Streaming使用离散化流(discretized stream)作为抽象表示，叫作DStream。

DStream 是随时间推移而收到的数据的序列。在内部，每个时间区间收到的数据都作为 RDD 存在，而DStream是由这些RDD所组成的序列(因此得名“离散化”)。



##### 2. SparkStreaming特点

1. 易用
2. 容错
3. 易整合到Spark体系

##### 3. SparkStreaming架构

![](https://img-blog.csdnimg.cn/20200630194851973.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NDg2NTU3NA==,size_16,color_FFFFFF,t_70)



#### 二. Dstream 

##### 1. WordCount 实操

1. 添加sparkStream 依赖

~~~xml
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-streaming_2.11</artifactId>
    <version>2.1.1</version>
</dependency>

~~~

2. 编写代码

~~~java
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
~~~



##### 2. Dstream 创建

###### 2.1 文件数据源

文件数据流：能够读取所有HDFS API兼容的文件系统文件，通过fileStream方法进行读取，Spark Streaming 将会监控 dataDirectory 目录并不断处理移动进来的文件，记住目前不支持嵌套目录。

`streamingContext.textFileStream(dataDirectory)`

注意事项：

1. 文件需要有相同的数据格式；

2. 文件进入 dataDirectory的方式需要通过移动或者重命名来实现；

3. 一旦文件移动进目录，则不能再修改，即便修改了也不会读取新数据；

~~~java
/**
  * 从File中采集数据
  */
object B_FileDataSource {
  def main(args: Array[String]): Unit = {
    //1.初始化SparkConf
    val conf: SparkConf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("Spark Streaming Demo")
    //2.实时数据分析环境对象
    //采集周期：以指定的时间为周期采集实时数据
    val ssc = new StreamingContext(conf,Seconds(5))
    //远程监控端口
    val rid: DStream[String] = ssc.textFileStream("in/sparkstream/test")


    //解析
    val dstream: DStream[String] = rid.flatMap(_.split(" "))
    val dstream2: DStream[(String, Int)] = dstream.map((_,1)).reduceByKey(_+_)
    dstream2.print()

    //启动SparkStreaming最重要的设置
    //1.启动采集器
    ssc.start()
    //2.driver等待采集器执行
    ssc.awaitTermination()

  }
}
~~~

###### 2.2 RDD队列

使用ssc.queueStream(queueOfRDDs)来创建DStream，每一个推送到这个队列中的RDD，都会作为一个DStream处理

~~~java
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
~~~

###### 2.3 自定义数据源

需要继承Receiver，并实现onStart、onStop方法来自定义数据源采集。

~~~java
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
~~~

###### 2.4 kafka 数据源

1. 导入依赖

~~~xml
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-streaming-kafka-0-10_2.11</artifactId>
    <version>2.4.3</version>
</dependency>
~~~

2. 简单版

~~~java
/**
  * Spark Streaming 使用kafka作为数据源
  */
object E_DataSourceFromKafka {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("KafkaStreaming")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(3))
    val topics = Array("xiahu2")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node3:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "default",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition(recordIt => {
        for (record <- recordIt) {
          println(record.value())
        }
      })
      // some time later, after outputs have completed
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
~~~

3. 复杂版就是在简单版内添加其他逻辑



##### 3. Dstream 转换

###### 3.1 无状态转化操作

无状态转化操作就是把简单的RDD转化操作应用到每个批次上，也就是转化DStream中的每一个RDD。部分无状态转化操作列在了下表中。

注意，针对键值对的DStream转化操作(比如reduceByKey())要添加import StreamingContext._才能在Scala中使用

###### 3.2 有状态转化操作（重点）

直接看代码

~~~java
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
~~~

###### 3.3 window

WindowOperations可以设置窗口的大小和滑动窗口的间隔来动态的获取当前Steaming的允许状态。

基于窗口的操作会在一个比StreamingContext 的批次间隔更长的时间范围内，通过整合多个批次的结果，计算出整个窗口的结果。

**注意**：所有基于窗口的操作都需要两个参数，分别为窗口时长以及滑动步长，两者都必须是 StreamContext 的批次间隔的整数倍。

1. scala 内的滑步操作

~~~java
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
~~~

2. spark 内的 窗口操作

~~~java
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
~~~

###### 3.4 Transform

Transform原语允许DStream上执行任意的RDD-to-RDD函数。

即使这些函数并没有在DStream的API中暴露出来，通过该函数可以方便的扩展Spark API。

该函数每一批次调度一次。其实也就是对DStream中的RDD应用转换。

~~~java
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
~~~

###### 3.5 Join

连接操作（leftOuterJoin, rightOuterJoin, fullOuterJoin也可以），可以连接

1. Stream-Stream
2. windows-stream to windows-stream
3. stream-dataset

~~~java
val stream1: DStream[String, String] = ...
val stream2: DStream[String, String] = ...
val joinedStream = stream1.join(stream2)

val windowedStream1 = stream1.window(Seconds(20))
val windowedStream2 = stream2.window(Minutes(1))
val joinedStream = windowedStream1.join(windowedStream2)
    
//Stream-dataset joins
val dataset: RDD[String, String] = ...
val windowedStream = stream.window(Seconds(20))...
val joinedStream = windowedStream.transform { rdd => rdd.join(dataset) }

~~~

##### 4. Dstream 输出

输出操作指定了对流数据经转化操作得到的数据所要执行的操作(例如把结果推入外部数据库或输出到屏幕上)。

与RDD中的惰性求值类似，如果一个DStream及其派生出的DStream都没有被执行输出操作，那么这些DStream就都不会被求值。

如果StreamingContext中没有设定输出操作，整个context就都不会启动。 

###### 4.1 print()

在运行流程序的驱动结点上打印DStream中每一批次数据的最开始10个元素。这用于开发和调试。在Python API中，同样的操作叫print()

###### 4.2 saveAsTextFiles(prefix,[suffix])

以text文件形式存储这个DStream的内容。每一批次的存储文件名基于参数中的prefix和suffix。

”prefix-Time_IN_MS[.suffix]”.

###### 4.3 saveAsObjectFiles(prefix,[suffix])
以Java对象序列化的方式将Stream中的数据保存为 SequenceFiles . 每一批次的存储文件名基于参数中的为"prefix-TIME_IN_MS[.suffix]". Python中目前不可用。

###### 4.4 saveAsHadoopFiles(prefix,[suffix])

将Stream中的数据保存为Hadoop files. 每一批次的存储文件名基于参数中的为"prefix-TIME_IN_MS[.suffix]"

###### 4.5 foreachRDD(func)

这是最通用的输出操作，即将函数 func 用于产生于 stream的每一个RDD。其中参数传入的函数func应该实现将每一个RDD中数据推送到外部系统，如将RDD存入文件或者通过网络将其写入数据库。

注意：函数func在运行流应用的驱动中被执行，同时其中一般函数RDD操作从而强制其对于流RDD的运算。

通用的输出操作foreachRDD()，它用来对DStream中的RDD运行任意计算。这和transform() 有些类似，都可以让我们访问任意RDD。在foreachRDD()中，可以重用我们在Spark中实现的所有行动操作。

比如，常见的用例之一是把数据写到诸如MySQL的外部数据库中。 注意：

（1）连接不能写在driver层面；

（2）如果写在foreach则每个RDD都创建，得不偿失；

（3）增加foreachPartition，在分区创建。使用foreachPartition，在该算子下构造逻辑