# B_Spark_RDD（上）

## 一. RDD概述

## 1. 什么是RDD

RDD（Resilient DistributedDataset）叫做分布式数据集，是Spark中最基本的数据抽象。

代码中是一个抽象类，它代表一个不可变、可分区、里面的元素可并行计算的集合。

## 2. RDD的属性

1. 一组分区（Partition），即数据集的基本组成单位
2. 一个计算每个分区的函数
3. RDD之间的依赖关系
4. 一个Partitioner，即RDD的分片函数
5. 一个列表，存储存放每个Partition的优先位置

## 3. RDD的特点

RDD表示只读的分区的数据集，对RDD进行改动，只能通过RDD的转换操作，由一个RDD得到一个新的RDD，新的RDD包含了从其他RDD衍生所必需的信息。

RDDs之间存在依赖，RDD的执行是按照血缘关系延时计算的。如果血缘关系较长，可以通过持久化RDD来切断血缘关系

### 3.3.1 分区

~~~
RDD逻辑上是分区的，每个分区的数据是抽象存在的，计算的时候会通过一个compute函数得到每个分区的数据。
如果RDD是通过已有的文件系统构建，则compute函数是读取指定文件系统中的数据；
如果RDD是通过其他RDD转换而来，则compute函数是执行转换逻辑将其他RDD的数据进行转换。
~~~

![](https://img-blog.csdnimg.cn/20200305204636903.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NDg2NTU3NA==,size_16,color_FFFFFF,t_70)

### 3.3.2 只读

如下图所示，RDD是只读的，要想改变RDD中的数据，只能在现有的RDD基础上创建新的RDD;

![](https://img-blog.csdnimg.cn/20200305204806701.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NDg2NTU3NA==,size_16,color_FFFFFF,t_70)

由一个RDD转换到另一个RDD，可以通过丰富的操作算子实现，不再像MapReduce那样只能写map和reduce了，如下图所示:



![](https://img-blog.csdnimg.cn/20200305205234826.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NDg2NTU3NA==,size_16,color_FFFFFF,t_70)

RDD的操作算子包括两类，

1. transformations，它是用来将RDD进行转化，构建RDD的血缘关系；
2. actions，它是用来触发RDD的计算，得到RDD的相关计算结果或者将RDD保存的文件系统中；

### 3.3.3 依赖

RDDs通过操作算子进行转换，转换得到的新RDD包含了从其他RDDs衍生所必需的信息，RDDs之间维护着这种血缘关系，也称之为依赖。

如下图所示，依赖包括两种：

1. 窄依赖，RDDs之间分区是一一对应的，
2. 宽依赖，下游RDD的每个分区与上游RDD(也称之为父RDD)的每个分区都有关，是多对多的关系

![](https://img-blog.csdnimg.cn/20200305204824869.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NDg2NTU3NA==,size_16,color_FFFFFF,t_70)

### 3.3.4 缓存

如果在应用程序中多次使用同一个RDD，可以将该RDD缓存起来，该RDD只有在第一次计算的时候会根据血缘关系得到分区的数据，在后续其他地方用到该RDD的时候，会直接从缓存处取而不用再根据血缘关系计算，这样就加速后期的重用。

如下图所示，RDD-1经过一系列的转换后得到RDD-n并保存到hdfs，RDD-1在这一过程中会有个中间结果，如果将其缓存到内存，那么在随后的RDD-1转换到RDD-m这一过程中，就不会计算其之前的RDD-0了

![](https://img-blog.csdnimg.cn/20200305205346270.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NDg2NTU3NA==,size_16,color_FFFFFF,t_70)

### 3.3.5 CheakPoint

虽然RDD的血缘关系天然地可以实现容错，当RDD的某个分区数据失败或丢失，可以通过血缘关系重建。

但是对于长时间迭代型应用来说，随着迭代的进行，RDDs之间的血缘关系会越来越长，一旦在后续迭代过程中出错，则需要通过非常长的血缘关系去重建，势必影响性能。

为此，RDD支持checkpoint将数据保存到持久化的存储中，这样就可以切断之前的血缘关系，因为checkpoint后的RDD不需要知道它的父RDDs了，它可以从checkpoint处拿到数据

## 4. 理解RDD的实现

### 4.1 IO实现

![](https://img-blog.csdnimg.cn/20200305210050229.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NDg2NTU3NA==,size_16,color_FFFFFF,t_70)

### 4.2 RDD实现

![](https://img-blog.csdnimg.cn/20200305210110737.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NDg2NTU3NA==,size_16,color_FFFFFF,t_70)

### 4.3 Driver与Executor的关系

![](https://img-blog.csdnimg.cn/20200305210737125.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NDg2NTU3NA==,size_16,color_FFFFFF,t_70)

## 5. RDD编程模型

在Spark中，RDD被表示为对象，通过对象上的方法调用来对RDD进行转换。

经过一系列的transformations定义RDD之后，就可以调用actions触发RDD的计算，action可以是向应用程序返回结果(count, collect等)，或者是向存储系统保存数据(saveAsTextFile等)。

在Spark中，只有遇到action，才会执行RDD的计算(即延迟计算)，这样在运行时可以通过管道的方式传输多个转换。

### 5.1 RDD的创建

~~~scala
//Spark提供的三种方式创建

//1.从内存中parallelize
val rddOne: RDD[Int] = sc.parallelize(Array(1,2,3,4,5,6,7,8))
//2.从内存中makeRDD(底层也是勇敢parallelize实现)
val rddTwo: RDD[Int] = sc.makeRDD(Array(1,2,3,4,5,6,7,8))
//3.从外部存储中创建
val rddThree: RDD[String] = sc.textFile("path")

//RDD在创建时，是可以指定RDD的分区个数的，例如:
val rddTwo2: RDD[Int] = sc.makeRDD(Array(1,2,3,4,5,6,7,8),2)
//上述代码表示创建2分区的RDD.
//如果没有指定分区，默认是使用：defaultParallelism
//比如你在创建SparkConf对象时是会setMaster("Local[3]")，例如：
val conf = new SparkConf().setMaster("local[3]").setAppName("Word Count")
//此时：defaultParallelism = 3，所以如果在创建RDD时没有指定分区，Spark会默认创建3分区的RDD
//但是，如果你使用--从外部存储中创建RDD，需要注意如下：
//读取文件时,传递的分区参数为最小分区数量,但是不一定时这个分区数，例如：
val fileRdd: RDD[String] = sc.textFile("path",2)
//此时指定的分区个数=2，但是最终的分区数量不一定是2，这个是取决于hadoop读取文件时分片规则
~~~

### 5.2 RDD的Transform

RDD整体上分为Value类型和Key-Value类型

#### 5.2.1 Value类型

##### 5.2.1.1 map

![](https://img-blog.csdnimg.cn/20200305212748102.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NDg2NTU3NA==,size_16,color_FFFFFF,t_70)

~~~scala
package cn.xhjava.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 返回一个新的RDD，该RDD由每一个输入元素经过func函数转换后组成
  */
object Spark_RDD_map {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Word Count")
    val sc = new SparkContext(conf)

    val listRdd: RDD[Int] = sc.makeRDD(1 to 10)


    val partitionRdd: RDD[Int] = listRdd.map((_ * 2))

    //收集并且打印
    partitionRdd.collect().foreach(println)
  }
}

~~~

##### 5.2.1.2 mapPartition

![](https://img-blog.csdnimg.cn/20200305212808106.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NDg2NTU3NA==,size_16,color_FFFFFF,t_70)

~~~scala
package cn.xhjava.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 类似于map，但独立地在RDD的每一个分片上运行，因此在类型为T的RDD上运行时，
  * func的函数类型必须是Iterator[T] => Iterator[U]。
  * 假设有N个元素，有M个分区，那么map的函数的将被调用N次,
  * 而mapPartitions被调用M次,一个函数一次处理所有分区。
  */
object Spark_RDD_mapPartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Word Count")
    val sc = new SparkContext(conf)

    val listRdd: RDD[Int] = sc.makeRDD(1 to 10)

    // mapPartition可以对RDD中所有的分区进行遍历
    // mapPartition效率优先map算子,减少了执行器执行交互次数
    // mapPartition可能出现内存溢出(OOM)
    val partitionRdd: RDD[Int] = listRdd.mapPartitions(datas => {
      datas.map(_ * 2)
    })
    //收集并且打印
    partitionRdd.collect().foreach(println)
  }
}

//map与mapPartition的区别
//1. map()：每次处理一条数据；
//2. mapPartition()：每次处理一个分区的数据，这个分区的数据处理完后，原RDD中分区的数据才能释放，可能导致OOM；
//3. 当内存空间较大的时候建议使用mapPartition()，以提高处理效率；
~~~

##### 5.2.1.3 mapPartitionWithIndex

![](https://img-blog.csdnimg.cn/20200305212819707.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NDg2NTU3NA==,size_16,color_FFFFFF,t_70)

~~~scala
package cn.xhjava.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 类似于mapPartitions，但func带有一个整数参数表示分片的索引值，
  * 因此在类型为T的RDD上运行时，func的函数类型必须是(Int, Interator[T]) => Iterator[U]；
  */
object Spark_RDD_mapPartitionWithIndex {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Word Count")
    val sc = new SparkContext(conf)
    //1.创建数据,5分区
    val mapRdd: RDD[Int] = sc.makeRDD(1 to 10,5)
    //mapPartitionWithIndex
    val tupleRdd: RDD[(Int, String)] = mapRdd.mapPartitionsWithIndex {
      case (partitionNum, datas) => {
        datas.map((_, "分区:" + partitionNum))
      }
    }

    //收集并且打印
    tupleRdd.collect().foreach(println)
  }
}

~~~

##### 5.2.1.4 flatMap

~~~scala
package cn.xhjava.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 类似于map，但是每一个输入元素可以被映射为0或多个输出元素
  * （所以func应该返回一个序列，而不是单一元素）
  */
object Spark_RDD_flatMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Word Count")
    val sc = new SparkContext(conf)

    val listRdd: RDD[List[Int]] = sc.makeRDD(Array(List(1, 2), List(3, 4)))

    val flatMapRdd: RDD[Int] = listRdd.flatMap(datas => datas)
    flatMapRdd.collect().foreach(println)
  }
}

~~~

##### 5.2.1.5 glom

~~~scala
package cn.xhjava.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *将每一个分区形成一个数组，形成新的RDD类型时RDD[Array[T]
  */
object Spark_RDD_glom {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Word Count")
    val sc = new SparkContext(conf)

    val listRdd: RDD[Int] = sc.makeRDD(1 to 16, 4)


    //将一个分区中的数据放入一个数组中
    val glomRDD: RDD[Array[Int]] = listRdd.glom()

    //收集并且打印
    glomRDD.collect().foreach(array => {
      println(array.mkString(","))
    })
  }
}

~~~

##### 5.2.1.6 groupBy

~~~scala
package cn.xhjava.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 分组，按照传入函数的返回值进行分组。将相同的key对应的值放入一个迭代器。
  */
object Spark_RDD_groupBy {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Word Count")
    val sc = new SparkContext(conf)

    //生成数据，按照指定规则进行分区
    val listRdd: RDD[Int] = sc.makeRDD(1 to 10)

    // 分组后的数据形成了对偶元组(K-V),K表示分组的key,V表示分组后的数据元组
    val groupByRdd: RDD[(Int, Iterable[Int])] = listRdd.groupBy(_ % 2)

    //收集并且打印
    groupByRdd.collect().foreach(println)
  }
}

~~~

##### 5.2.1.7 filter

~~~scala
package cn.xhjava.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 过滤。
  * 返回一个新的RDD，该RDD由经过func函数计算后返回值为true的输入元素组成。
  */
object Spark_RDD_filter {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Word Count")
    val sc = new SparkContext(conf)

    val listRdd: RDD[Int] = sc.makeRDD(1 to 10)


    val filterRdd: RDD[Int] = listRdd.filter(_ % 2 == 0)

    //收集并且打印
    filterRdd.collect().foreach(println)
  }
}

~~~

##### 5.2.1.8 sample

~~~scala
package cn.xhjava.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 随机抽样
  */
object Spark_RDD_sample {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Word Count")
    val sc = new SparkContext(conf)

    val listRdd: RDD[Int] = sc.makeRDD(1 to 10)

    // withReplacement:boolean类型.抽出的数据是否放回;true/false :放回/不放回
    // fraction:Double类型,内容只能位于[0-1]之间.类似于提供一个标准.
    // seed:随机数生成器的种子

    //从指定的数据集合中进行抽样处理,根据不同的算法进行抽象
    // 放回抽样
    // val sampleRdd: RDD[Int] = listRdd.sample(false,1,1)
    //不放回抽样
    val sampleRdd: RDD[Int] = listRdd.sample(true, 5, 2)
    //收集并且打印
    sampleRdd.collect().foreach(println)
  }
}

~~~

##### 5.2.1.9 distinct

![](https://img-blog.csdnimg.cn/20200305212829176.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NDg2NTU3NA==,size_16,color_FFFFFF,t_70)

~~~scala
package cn.xhjava.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 对源RDD进行去重后返回一个新的RDD。
  * 默认情况下，只有8个并行任务来操作，但是可以传入一个可选的numTasks参数改变它。
  */
object Spark_RDD_distinct {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Word Count")
    val sc = new SparkContext(conf)
    val listRdd: RDD[Int] = sc.makeRDD(Array(1, 3, 2, 5, 6, 1, 6, 5, 2))

    // val distinctRdd: RDD[Int] = listRdd.distinct()
    //使用distinct算子对数据进行去重,但是因为去重后会导致数据减少,所有可以改变默认的分区数量
    // 该算子存在一个shuffle的行为
    val distinctRdd: RDD[Int] = listRdd.distinct(3)

    //收集并且打印
    distinctRdd.collect().foreach(println)
  }
}
~~~

##### 5.2.1.10 coalesce

~~~scala
package cn.xhjava.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 缩减分区数，用于大数据集过滤后，提高小数据集的执行效率。
  */
object Spark_RDD_coalesce {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Word Count")
    val sc = new SparkContext(conf)
    //创建一个4分区的RDD
    val listRdd: RDD[Int] = sc.makeRDD(1 to 10, 4)
    println("缩减分区前的分区数量:" + listRdd.partitions.size)

    val coalesceRdd: RDD[Int] = listRdd.coalesce(2)
    println("缩减分区后的分区数量:" + coalesceRdd.partitions.size)

    //收集并且打印
    coalesceRdd.collect().foreach(println)
  }
}
~~~

##### 5.2.1.11 repartition 

~~~scala
package cn.xhjava.spark.rdd.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 根据分区数，重新通过网络随机洗牌所有数据。
  */
object Spark_RDD_repartition {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Word Count")
    val sc = new SparkContext(conf)

    //创建4个分区的RDD
    val listRdd: RDD[Int] = sc.makeRDD(1 to 16, 4)
    println("重新分区前分区数:" + listRdd.partitions.size)
    val repartitionRDD: RDD[Int] = listRdd.repartition(2)
    println("重新分区前分区数:" + repartitionRDD.partitions.size)
    //收集并且打印
    repartitionRDD.collect().foreach(println)

    // coalesce和repartition的区别
    // coalesce重新分区，可以选择是否进行shuffle过程
    // repartition:通过源码发现,强制进行shuffle.
  }
}

~~~



##### 5.2.1.12 sortBy

~~~scala
package cn.xhjava.spark.rdd.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 使用func先对数据进行处理，按照处理后的数据比较结果排序，默认为正序。
  */
object Spark_RDD_sortby {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Word Count")
    val sc = new SparkContext(conf)

    val listRdd: RDD[Int] = sc.makeRDD(List(1, 3, 6, 5, 3, 6, 2, 9, 6, 7, 8))

    //按照与3余数的大小排序
    //val sortByRDD: RDD[Int] = listRdd.sortBy(_ % 3)
    //按照自身数据大小排序,倒序排序
    val sortByRDD: RDD[Int] = listRdd.sortBy(X => X,false)


    //收集并且打印
    sortByRDD.collect().foreach(println)
  }
}

~~~

##### 5.2.1.13 pipe

~~~scala
package cn.xhjava.spark.rdd.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 管道，针对每个分区，都执行一个shell脚本，返回输出的RDD
  * 注意：脚本需要放在Worker节点可以访问到的位置
  */
object Spark_RDD_pipe {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Word Count")
    val sc = new SparkContext(conf)

    val listRdd: RDD[String] = sc.parallelize(List("hi", "Hello", "how", "are", "you"), 2)

    //需要在Linux机器上测试
    val pipeRDD: RDD[String] = listRdd.pipe("E:\\Tmp\\Spark_pipe_test.sh")

    //收集并且打印
    pipeRDD.collect().foreach(println)
  }
}

~~~

#### 5.2.2 Value-Value类型

##### 5.2.2.1 union 

~~~scala
package cn.xhjava.spark.rdd.value_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 对源RDD和参数RDD求并集后返回一个新的RDD
  * 求两个RDD的并集
  */
object Spark_RDD_union {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Word Count")
    val sc = new SparkContext(conf)

    //创建第一个RDD
    val listRdd: RDD[Int] = sc.makeRDD(1 to 5)
    //创建第二个RDD
    val listRdd2: RDD[Int] = sc.makeRDD(10 to 15)

    val unionRDD: RDD[Int] = listRdd.union(listRdd2)


    //收集并且打印
    unionRDD.collect().foreach(println)
  }
}

~~~

##### 5.2.2.2 subtract

~~~scala
package cn.xhjava.spark.rdd.value_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 计算差的一种函数，去除两个RDD中相同的元素，不同的RDD将保留下来
  * 求两个RDD的差集
  */
object Spark_RDD_subtract {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Word Count")
    val sc = new SparkContext(conf)

    //创建第一个RDD
    val listRdd: RDD[Int] = sc.makeRDD(1 to 10)
    //创建第二个RDD
    val listRdd2: RDD[Int] = sc.makeRDD(10 to 15)

    val subtractRDD: RDD[Int] = listRdd.subtract(listRdd2)

    //收集并且打印
    subtractRDD.collect().foreach(println)
  }
}

~~~

##### 5.2.2.3 intersection

~~~scala
package cn.xhjava.spark.rdd.value_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 对源RDD和参数RDD求交集后返回一个新的RDD
  * 求两个RDD的交集
  */
object Spark_RDD_intersection {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Word Count")
    val sc = new SparkContext(conf)

    //创建第一个RDD
    val listRdd: RDD[Int] = sc.makeRDD(1 to 10)
    //创建第二个RDD
    val listRdd2: RDD[Int] = sc.makeRDD(5 to 15)

    val intersectionRDD: RDD[Int] = listRdd.intersection(listRdd2)


    //收集并且打印
    intersectionRDD.collect().foreach(println)
  }
}

~~~



##### 5.2.2.4 cartesian

~~~scala
package cn.xhjava.spark.rdd.value_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 笛卡尔积（尽量避免使用）
  * 创建两个RDD，计算两个RDD的笛卡尔积
  */
object Spark_RDD_cartesian {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Word Count")
    val sc = new SparkContext(conf)

    //创建第一个RDD
    val listRdd: RDD[Int] = sc.makeRDD(1 to 3)
    //创建第二个RDD
    val listRdd2: RDD[Int] = sc.makeRDD(2 to 6)

    val cartesianRDD: RDD[(Int, Int)] = listRdd.cartesian(listRdd2)


    //收集并且打印
    cartesianRDD.collect().foreach(println)
  }
}

~~~

##### 5.2.2.5 zip

~~~scala
package cn.xhjava.spark.rdd.value_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将两个RDD组合成Key/Value形式的RDD
  * 这里默认两个RDD的partition数量以及元素数量都相同，否则会抛出异常。
  * 创建两个RDD，并将两个RDD组合到一起形成一个(k,v)RDD
  */
object Spark_RDD_zip {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Word Count")
    val sc = new SparkContext(conf)

    //创建第一个RDD
    val listRdd: RDD[Int] = sc.makeRDD(1 to 5,5)
    //创建第二个RDD
    val listRdd2: RDD[String] = sc.makeRDD(List("hello","word","my","name","xiahu"),5)

    val zipRDD: RDD[(Int, String)] = listRdd.zip(listRdd2)

    //val listRdd: RDD[Int] = sc.makeRDD(1 to 5,5)
    //val listRdd2: RDD[String] = sc.makeRDD(List("hello","word","my","name"),4)
    //上述情况会报错


    //收集并且打印
    zipRDD.collect().foreach(println)
  }
}

~~~

#### 5.2.3 Key-Value类型

##### 5.2.3.1 partitionBy

~~~scala
package cn.xhjava.spark.rdd.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * 对pairRDD进行分区操作，如果原有的partionRDD和现有的partionRDD是一致的话就不进行分区，
  * 否则会生成ShuffleRDD，即会产生shuffle过程。
  */
object Spark_RDD_partitionBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Word Count")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "c")), 3)
    //分区
    val partitionRDD: RDD[(Int, String)] = rdd.partitionBy(new MyPartitioner(2))
    partitionRDD.saveAsTextFile("output")
  }
}


/**
  * 自定义分区器
  * 继承Partitioner
  */
class MyPartitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = {
    partitions
  }

  //返回分区索引,将所有数据存入分区1
  override def getPartition(key: Any): Int = {
    1
  }
}
~~~

##### 5.2.3.2 groupByKey

~~~scala
package cn.xhjava.spark.rdd.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * groupByKe对每个key进行操作，只生成一个sequence(集合)
  */
object Spark_RDD_groupByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Word Count")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[String] = sc.makeRDD(Array("one", "two", "two", "three", "three", "three"))
    //使用map转换结构
    val mapRDD: RDD[(String, Int)] = rdd.map((_, 1))
    //groupByKey
    val groupByKeyRdd: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
    //打印结果:
    //(two,CompactBuffer(1, 1))
    //(one,CompactBuffer(1))
    //(three,CompactBuffer(1, 1, 1))
    //计算相同key分组后的累加值
    val result: RDD[(String, Int)] = groupByKeyRdd.map(t => (t._1, t._2.sum))
    //打印结果:
    //    (two,2)
    //    (one,1)
    //    (three,3)

    result.collect().foreach(println)
  }
}
~~~

##### 5.2.3.3 reduceByKey

~~~scala
package cn.xhjava.spark.rdd.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * reduceByKey(func, [numTasks])
  * 在一个(K,V)的RDD上调用，返回一个(K,V)的RDD，
  * 使用指定的reduce函数，将相同key的值聚合到一起，reduce任务的个数可以通过numTasks来设置
  */
object Spark_RDD_reduceByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Word Count")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("female", 1), ("male", 5), ("female", 5), ("male", 2)))
    //reduceByKey,将key相同的直接进行reduce计算
    val reduceByKeyRDD: RDD[(String, Int)] = rdd.reduceByKey((x, y) => x * y)
    //打印结果:
//    (male,10)
//    (female,5)

    reduceByKeyRDD.collect().foreach(println)


  }
  //reduceByKey和groupByKey的区别
  //reduceByKey：按照key进行聚合，在shuffle之前有combine（预聚合）操作，返回结果是RDD[k,v].
  //groupByKey：按照key进行分组，直接进行shuffle。
  //性能推荐：reduceByKey优先使用,需要注意是否影响业务实现
}
~~~

##### 5.2.3.4 aggregateByKey

~~~scala
package cn.xhjava.spark.rdd.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 在kv对的RDD中,按key将value进行分组合并;
  * 合并时，将每个value和初始值作为seq函数的参数进行计算,返回的结果作为一个新的kv对;
  * 然后再将结果按照key进行合并，
  * 最后将每个分组的value传递给combine函数进行计算
  * （先将前两个value进行计算，将返回结果和下一个value传给combine函数，以此类推），
  * 将key与计算结果作为一个新的kv对输出。
  */
object Spark_RDD_aggregateByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Word Count")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)

    //在学习该RDD之前,先理解什么是：区间内,区间间
    //我们在创建RDD时,会指定分区的个数,上面一行代码我们就创建了分区为2的RDD,
    //1.区间内:那区间内呢？就是我们在分区1内部的空间，或者说时分区2内部的空间
    //2.区间间: 区间1与区间2也是存在与一块空间下,那么所以这块空间就是区间间

    /**
      * 该RDD参数说明
      * zeroValue：给每一个分区中的每一个key一个初始值；
      * seqOp：区间内的迭代计算
      * combOp：区间间的合并结果
      */

    val aggregateByKeyRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)(math.max(_, _), _ + _)
    //上面一行代码说明:
    //zeroValue=0,每个分区中每个key的初始值:0
    //math.max(_,_): 取出每个分区内,每个key的最大值
    //_+_:将区间间的数据进行相加
    aggregateByKeyRDD.collect().foreach(println)
    //打印结果:
    //    (b,3)
    //    (a,3)
    //    (c,12)


  }
}
~~~

如果不理解，请参考下图:

![](https://img-blog.csdnimg.cn/20200315202335453.jpg?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NDg2NTU3NA==,size_16,color_FFFFFF,t_70)

##### 5.2.3.5 foldByKey

~~~scala
package cn.xhjava.spark.rdd.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * aggregateByKey的简化操作
  */
object Spark_RDD_foldByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Word Count")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)

    /**
      * 该RDD参数说明
      * zeroValue：给每一个分区中的每一个key一个初始值；
      * seqOp：区间内的迭代计算
      * combOp：区间间的合并结果
      */

    val foldByKeyRDD: RDD[(String, Int)] = rdd.foldByKey(0)(_ + _)
    //上面一行代码说明:
    //zeroValue=0,每个分区中每个key的初始值:0
    //_ + _: 讲每个分区内key相同的value进行相加,最后将各分区key相同的value进行相加

    //rdd.aggregateByKey(0)(_ + _, _ + _) = rdd.foldByKey(0)(_ + _)


    foldByKeyRDD.collect().foreach(println)
    //打印结果:
    //    (b,3)
    //    (a,5)
    //    (c,18)


  }
}
~~~

##### 5.2.3.6 combineByKey

~~~scala
package cn.xhjava.spark.rdd.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 对相同K，把V合并成一个集合。
  */
object Spark_RDD_combineByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Word Count")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)), 2)

    /**
      * 参数描述:
      * createCombiner:
      * combineByKey() 会遍历分区中的所有元素，因此每个元素的键要么还没有遇到过，要么就和之前的某个元素的键相同。
      * 如果这是一个新的元素,combineByKey()会使用一个叫作createCombiner()的函数来创建那个键对应的累加器的初始值
      * mergeValue:
      * 如果这是一个在处理当前分区之前已经遇到的键，它会使用mergeValue()方法将该键的累加器对应的当前值与这个新的值进行合并
      * mergeCombiners:
      * 由于每个分区都是独立处理的， 因此对于同一个键可以有多个累加器。
      * 如果有两个或者更多的分区都有对应同一个键的累加器， 就需要使用用户提供的 mergeCombiners() 方法将各个分区的结果进行合并。
      */

    //需求:
    //创建一个pairRDD，根据key计算每种key的均值(先计算每个key出现的次数以及可以对应值的总和，再相除得到结果）
    val combineByKeyRDD: RDD[(String, (Int, Int))] = rdd.combineByKey(
      (_, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))

    //(_, 1):将每个分区的每个(Key,value)中的value进行map操作,将key对应的value转成tuple
    // (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1):元组的第一位与V相加，第二位自增1
    //(acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))：元组的第一位与第二位分别累加

    val resultRDD: RDD[(String, Double)] = combineByKeyRDD.map {
      case (key, value) => (key, value._1 / value._2.toDouble)
    }


    resultRDD.collect().foreach(println)
    //打印结果:
    //    (b,95.33333333333333)
    //    (a,91.33333333333333)


  }
}
~~~

如果不理解请参考：

![](https://img-blog.csdnimg.cn/20200315202335453.jpg?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NDg2NTU3NA==,size_16,color_FFFFFF,t_70)



##### 5.2.3.7 sortByKey

~~~scala
package cn.xhjava.spark.rdd.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 在一个(K,V)的RDD上调用，K必须实现Ordered接口，返回一个按照key进行排序的(K,V)的RDD
  */
object Spark_RDD_sortByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Word Count")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[(Int,String)] = sc.makeRDD(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))
    //正序排序
    val descRDD: RDD[(Int, String)] = rdd.sortByKey(true)
    //倒叙排序
    val ascRDD: RDD[(Int, String)] = rdd.sortByKey(false)

    descRDD.collect().foreach(println)

  }
}
~~~

##### 5.2.3.8 mapValue

~~~scala
package cn.xhjava.spark.rdd.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 针对于(K,V)形式的类型只对V进行操作
  */
object Spark_RDD_mapValue {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Word Count")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (1, "d"), (2, "b"), (3, "c")))
    val mapValueRDD: RDD[(Int, String)] = rdd.mapValues(_ + "xiahu")

    mapValueRDD.collect().foreach(println)
    //打印：
    //    (1,axiahu)
    //    (1,dxiahu)
    //    (2,bxiahu)
    //    (3,cxiahu)

  }
}
~~~

##### 5.2.3.9 join

~~~scala
package cn.xhjava.spark.rdd.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 在类型为(K,V)和(K,W)的RDD上调用，返回一个相同key对应的所有元素对在一起的(K,(V,W))的RDD
  */
object Spark_RDD_join {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Word Count")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "c")))
    val rdd2: RDD[(Int, Int)] = sc.makeRDD(Array((1, 4), (2, 5), (3, 6)))

    val joinRDD: RDD[(Int, (String, Int))] = rdd.join(rdd2)
    joinRDD.collect().foreach(println)
    //打印：
    //    (3,(c,6))
    //    (1,(a,4))
    //    (2,(b,5))

  }
}
~~~

##### 5.2.3.10 cogroup

~~~scala
package cn.xhjava.spark.rdd.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 在类型为(K,V)和(K,W)的RDD上调用，返回一个(K,(Iterable<V>,Iterable<W>))类型的RDD
  */
object Spark_RDD_coGroup {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Word Count")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "c")))
    val rdd2: RDD[(Int, Int)] = sc.makeRDD(Array((1, 4), (2, 5), (3, 6)))

    val cogroupRDD: RDD[(Int, (Iterable[String], Iterable[Int]))] = rdd.cogroup(rdd2)
    cogroupRDD.collect().foreach(println)
    //打印：
    //    (3,(CompactBuffer(c),CompactBuffer(6)))
    //    (1,(CompactBuffer(a),CompactBuffer(4)))
    //    (2,(CompactBuffer(b),CompactBuffer(5)))

    //join与cogroup的区别:
    //join只会一一对应，比如说:rdd1 = sc.makeRDD(List((1,"a"),(2,"b")))
    //rdd2 = sc.makeRDD(List((1,"aa"),(2,"bb"),(3,"cc")))
    //此时join的时候,(3,"cc")直接被忽略,不会被join进去
    //单cogroup不会忽略

  }
}
~~~

### 5.3 RDD的Action

##### 5.3.1 reduce  

~~~scala
package cn.xhjava.spark.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 通过func函数聚集RDD中的所有元素，先聚合分区内数据，再聚合分区间数据
  */
object Spark_Action_reduce {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_Action_reduce")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[Int] = sc.makeRDD(1 to 10, 2)
    //创建RDD2
    val rdd2: RDD[(String, Int)] = sc.makeRDD(Array(("a", 1), ("a", 3), ("c", 3), ("d", 5)))

    //聚合rdd内的所有元素
    val reduceRDD: Int = rdd.reduce(_ + _)
    println(reduceRDD)
    //打印结果:55
    //聚合rdd2[String]所有数据
    val reduceRDD2: (String, Int) = rdd2.reduce((x,y) =>(x._1+y._1,x._2+y._2))
    println(reduceRDD2)
    //打印结果:(acda,12)

  }
}
~~~

##### 5.3.2 collect 

~~~scala
package cn.xhjava.spark.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 在驱动程序中，以数组的形式返回数据集的所有元素
  */
object Spark_Action_collect {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_Action_collect")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[Int] = sc.makeRDD(1 to 10, 2)

    rdd.collect().foreach(println)
    //打印结果:
    //    1
    //    2
    //    3
    //    4
    //    5
    //    6
    //    7
    //    8
    //    9
    //    10

  }
}
~~~

##### 5.3.3 count

~~~scala
package cn.xhjava.spark.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 返回RDD中元素的个数
  */
object Spark_Action_count {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_Action_count")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[Int] = sc.makeRDD(1 to 10, 2)

    println(rdd.count())
    //打印结果:10

  }
}

~~~



##### 5.3.4 first

~~~scala
package cn.xhjava.spark.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 返回RDD中元素的个数
  */
object Spark_Action_first {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_Action_first")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[Int] = sc.makeRDD(1 to 10, 2)

    println(rdd.first())
    //打印结果:1

  }
}

~~~



##### 5.3.5 take

~~~scala
package cn.xhjava.spark.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 返回一个由RDD的前n个元素组成的数组
  */
object Spark_Action_take {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_Action_take")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[Int] = sc.makeRDD(1 to 10, 2)

    rdd.take(3).foreach(println)
    //打印结果:
    //1
    //2
    //3

  }
}

~~~



##### 5.3.6 takeOrdered

~~~
package cn.xhjava.spark.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 返回该RDD排序后的前n个元素组成的数组
  */
object Spark_Action_takeOrdered {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_Action_takeOrdered")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[Int] = sc.makeRDD(1 to 10, 2)

    rdd.takeOrdered(4).foreach(println)
    //打印结果:
    //1
    //2
    //3
    //4

  }
}

~~~



##### 5.3.7 aggregate 

~~~scala
package cn.xhjava.spark.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * aggregate函数将每个分区里面的元素通过seqOp和初始值进行聚合，
  * 然后用combine函数将每个分区的结果和初始值(zeroValue)进行combine操作。
  * 这个函数最终返回的类型不需要和RDD中元素类型一致。
  *
  * 与aggregateByKey类似
  */
object Spark_Action_aggregate {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_Action_aggregate")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[Int] = sc.makeRDD(1 to 10, 2)

    println(rdd.aggregate(0)(_ + _, _ + _))
    //打印结果:55

  }
}

~~~



#####5.3.8 fold

~~~scala
package cn.xhjava.spark.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 折叠操作，aggregate的简化操作，seqop和combop一样
  */
object Spark_Action_fold {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_Action_fold")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[Int] = sc.makeRDD(1 to 10, 2)

    println(rdd.fold(0)(_ + _))
    //打印结果:55


  }
}

~~~



##### 5.3.9 saveAsTextFile

~~~scala
package cn.xhjava.spark.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将数据集的元素以textfile的形式保存到HDFS文件系统或者其他支持的文件系统，
  * 对于每个元素，Spark将会调用toString方法，将它装换为文件中的文本
  */
object Spark_Action_saveAsTextFile {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_Action_saveAsTextFile")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[Int] = sc.makeRDD(1 to 10, 2)

    rdd.saveAsTextFile("outpath")
    //打印结果:

  }
}

~~~



##### 5.3.10 saveAsSequenceFile

~~~scala
package cn.xhjava.spark.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将数据集中的元素以Hadoop sequencefile的格式保存到指定的目录下，
  * 可以使HDFS或者其他Hadoop支持的文件系统
  */
object Spark_Action_saveAsSequenceFile {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_Action_saveAsSequenceFile")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[Int] = sc.makeRDD(1 to 10, 2)


    //打印结果:

  }
}

~~~



##### 5.3.11 saveAsObjectFile

~~~
package cn.xhjava.spark.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 用于将RDD中的元素序列化成对象，存储到文件中
  */
object Spark_Action_saveAsObjectFile {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_Action_saveAsObjectFile")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[Int] = sc.makeRDD(1 to 10, 2)

    rdd.saveAsObjectFile("outpath")
    //打印结果:

  }
}

~~~



##### 5.3.12 countByKey

~~~scala
package cn.xhjava.spark.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 针对(K,V)类型的RDD，返回一个(K,Int)的map，表示每一个key对应的元素个数
  */
object Spark_Action_countByKey {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_Action_countByKey")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[(Int, Int)] = sc.makeRDD(List((1, 3), (1, 2), (1, 4), (2, 3), (3, 6), (3, 8)), 3)

    rdd.collect().foreach(println)
    //打印结果:
    //    (1,3)
    //    (1,2)
    //    (1,4)
    //    (2,3)
    //    (3,6)
    //    (3,8)

  }
}

~~~



##### 5.3.13 foreach

~~~scala
package cn.xhjava.spark.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 在数据集的每一个元素上，运行函数func进行更新
  */
object Spark_Action_foreach {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_Action_foreach")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[Int] = sc.makeRDD(1 to 10, 2)

    rdd.take(3).foreach(println)
    //打印结果:
    //1
    //2
    //3

  }
}

~~~







