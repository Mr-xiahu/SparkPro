# C_Spark_RDD（下）

## 一. RDD中的函数传递

### 1. 传递一个方法

~~~scala
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
~~~

### 2. 传递一个属性

~~~scala
package cn.xhjava.spark.rdd.funcationtransmit

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 在实际开发中我们往往需要自己定义一些对于RDD的操作，
  * 那么此时需要主要的是，初始化工作是在Driver端进行的，而实际运行程序是在Executor端进行的，
  * 这就涉及到了跨进程通信，是需要序列化的。
  */
object AttributeTransmit {
  def main(args: Array[String]): Unit = {
    //1.初始化配置信息及SparkContext
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    //2.创建一个RDD
    val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "atguigu"))

    //3.创建一个Search对象
    val search = new Search2("h")

    //4.运用第一个过滤函数并打印结果
    val match1: RDD[String] = search.getMatche2(rdd)
    match1.collect().foreach(println)

  }

}

class Search2(query: String){
  //过滤出包含字符串的数据
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  //过滤出包含字符串的RDD
  def getMatch1(rdd: RDD[String]): RDD[String] = {
    rdd.filter(isMatch)
  }



  //过滤出包含字符串的RDD
  def getMatche2(rdd: RDD[String]): RDD[String] = {
    rdd.filter(x => x.contains(query))
  }

  //在这个方法中所调用的变量：query 是定义在Search这个类中的字段，
  //实际上调用的是this.query，this表示Search这个类的对象，
  //程序在运行过程中需要将Search对象序列化以后传递到Executor端。

  //解决方法:
  //实现序列化接口

}
~~~



## 二. RDD依赖关系

### 1. Lineage(血缘)

RDD只支持粗粒度转换，即在大量记录上执行的单个操作。将创建RDD的一系列Lineage（血统）记录下来，以便恢复丢失的分区。RDD的Lineage会记录RDD的元数据信息和转换行为，当该RDD的部分分区数据丢失时，它可以根据这些信息来重新运算和恢复丢失的数据分区。

查看血缘的相关方法:

~~~scala
package cn.xhjava.spark.rdd.dependen

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 查看血缘关系所需要的方法
  */
object Spark_Dependen_Demo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_Dependen_Demo")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("a", "b", "c", "d", "a"))


    val mapRDD: RDD[(String, Int)] = rdd.map((_, 1))

    val reduceByKeyRDD: RDD[(String, Int)] = mapRDD.reduceByKey((_ + _))

    //查看血缘
    println(reduceByKeyRDD.toDebugString)
    //(3) ShuffledRDD[2] at reduceByKey at Spark_Dependen_Demo.scala:20 []
    //  +-(3) MapPartitionsRDD[1] at map at Spark_Dependen_Demo.scala:18 []
    //    |  ParallelCollectionRDD[0] at makeRDD at Spark_Dependen_Demo.scala:15 []

    //查看依赖类型
    println(reduceByKeyRDD.dependencies)
    //List(org.apache.spark.ShuffleDependency@10ef5fa0)

    //收集并且打印
    reduceByKeyRDD.collect().foreach(println)
  }
}

~~~

### 2. 窄依赖

窄依赖指的是每一个父RDD的Partition最多被子RDD的一个Partition使用

![](https://img-blog.csdnimg.cn/20200319202538614.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NDg2NTU3NA==,size_16,color_FFFFFF,t_70#pic_center)

### 3. 宽依赖

宽依赖指的是多个子RDD的Partition会依赖同一个父RDD的Partition，会引起shuffle

宽依赖一个父RDD有多个子RDD

![](https://img-blog.csdnimg.cn/20200319202606599.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NDg2NTU3NA==,size_16,color_FFFFFF,t_70#pic_center)

### 4. DAG

DAG(Directed Acyclic Graph)叫做有向无环图，原始的RDD通过一系列的转换就就形成了DAG，根据RDD之间的依赖关系的不同将DAG划分成不同的Stage，

对于窄依赖，partition的转换处理在Stage中完成计算。

对于宽依赖，由于有Shuffle的存在，只能在parent RDD处理完成后，才能开始接下来的计算，

因此**宽依赖是划分** Stage的依据。宽依赖会产生shuffer，确切来说，**shuff**是划分Stage的依据

![](https://img-blog.csdnimg.cn/20200319203254800.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NDg2NTU3NA==,size_16,color_FFFFFF,t_70)

### 5. 任务划分（面试）

RDD任务切分中间分为：Application、Job、Stage和Task

1. Application:初始化一个SparkContext即生成一个Application
2. Job : 一个Action算子就会生成一个Job
3. Stage : 根据RDD之间的依赖关系的不同将Job划分成不同的Stage，遇到一个宽依赖则划分一个Stage。
4. Task : Stage是一个TaskSet，将Stage划分的结果发送到不同的Executor执行即为一个Task。

注意：Application->Job->Stage->Task每一层都是1对n的关系



## 三. RDD缓存

~~~scala
package cn.xhjava.spark.rdd.cache

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * RDD通过persist方法或cache方法可以将前面的计算结果缓存，
  * 默认情况下 persist() 会把数据以序列化的形式缓存在 JVM 的堆空间中。
  * 但是并不是这两个方法被调用时立即缓存，而是触发后面的action时，
  * 该RDD将会被缓存在计算节点的内存中，并供后面重用。
  */
object Spark_RDD_cache_Demo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_Dependen_Demo")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("xiahu", "tom", "jeck"))
    //将rdd 通过map 转换为tuple
    val mapRDD: RDD[(String, Long)] = rdd.map((_, System.currentTimeMillis()))

    //多次打印结果
    mapRDD.collect().foreach(println)
    Thread.sleep(5000)
    mapRDD.collect().foreach(println)
    //此次打印的时间戳是不一致的




    /**
      * 缓存有可能丢失，或者存储于内存的数据由于内存不足而被删除，
      * RDD的缓存容错机制保证了即使缓存丢失也能保证计算的正确执行。
      * 通过基于RDD的一系列转换，丢失的数据会被重算，由于RDD的各个Partition是相对独立的，因此只需要计算丢失的部分即可，并不需要重算全部Partition。
      */

    //缓存当前mapRDD
    mapRDD.cache()
    //多次打印结果
    mapRDD.collect().foreach(println)
    Thread.sleep(5000)
    mapRDD.collect().foreach(println)

  }
}

~~~

## 四. checkpoint

spark中对于数据的保存除了持久化操作之外，还提供了一种检查点的机制，检查点（本质是通过将RDD写入Disk做检查点）是为了通过lineage做容错的辅助，lineage过长会造成容错成本过高，这样就不如在中间阶段做检查点容错，如果之后有节点出现问题而丢失分区，从做检查点的RDD开始重做Lineage，就会减少开销。检查点通过将数据写入到HDFS文件系统实现了RDD的检查点功能。
为当前RDD设置检查点。该函数将会创建一个二进制的文件，并存储到checkpoint目录中，该目录是用SparkContext.setCheckpointDir()设置的。在checkpoint的过程中，该RDD的所有依赖于父RDD中的信息将全部被移除。对RDD进行checkpoint操作并不会马上被执行，必须执行Action操作才能触发。

~~~scala
package cn.xhjava.spark.rdd.checkpoint

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark_RDD_checkpoint_Demo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_RDD_checkpoint_Demo")
    val sc = new SparkContext(conf)
    //设置checkpoint 位置
    sc.setCheckpointDir("checkpoint")

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //将rdd 通过map 转换为tuple
    val mapRDD: RDD[(Int, Long)] = rdd.map((_, 1))


    val result: RDD[(Int, Long)] = mapRDD.reduceByKey(_ + _)

    //设置checkPoint
    result.checkpoint()


    result.collect().foreach(println)

    //查看血缘
    println(result.toDebugString)
    //(3) ShuffledRDD[2] at reduceByKey at Spark_RDD_checkpoint_Demo.scala:20 []
    //|  ReliableCheckpointRDD[3] at collect at Spark_RDD_checkpoint_Demo.scala:26 []
    //该血缘直接与reduceByKey算子后的shuffer关联,不会与map算子的血缘关联,因为是从checkpoint文件夹获取的数据信息
  }
}
~~~

## 五. 键值对分区器

Spark目前支持Hash分区和Range分区，用户也可以自定义分区，Hash分区为当前的默认分区;

Spark中分区器直接决定了RDD中分区的个数、RDD中每条数据经过Shuffle过程属于哪个分区和Reduce的个数

**注意**

1. 只有Key-Value类型的RDD才有分区器的，非Key-Value类型的RDD分区器的值是None
2. 每个RDD的分区ID范围：0~numPartitions-1，决定这个值是属于那个分区的(参考数组索引)

### 5.1 获取RDD分区 

~~~scala
package cn.xhjava.spark.rdd.partition

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD


/**
  * 获取partition类型
  */
object Spark_partition_GetPartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_Dependen_Demo")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List((1, 1), (2, 2), (3, 3)))

    //查看RDD分区器
    rdd.partitions.foreach(println)
    //    org.apache.spark.rdd.ParallelCollectionPartition@691
    //    org.apache.spark.rdd.ParallelCollectionPartition@692
    //    org.apache.spark.rdd.ParallelCollectionPartition@693

    //使用HashPartitioner对RDD进行重新分区
    val rdd2: RDD[(Int, Int)] = rdd.partitionBy(new HashPartitioner(2))
    //再次查看分区器
    rdd2.partitions.foreach(println)
    //org.apache.spark.rdd.ShuffledRDDPartition@0
    //org.apache.spark.rdd.ShuffledRDDPartition@1
  }
}

~~~

### 5.2 Hash分区

HashPartitioner分区的原理：

对于给定的key，计算其hashCode，并除以分区的个数取余，如果余数小于0，则用余数+分区的个数（否则加0），最后返回的值就是这个key所属的分区ID。

~~~scala
package cn.xhjava.spark.rdd.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}


/**
  * HashPartition
  */
object Spark_partition_HashPartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_Dependen_Demo")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List((1, 1), (2, 2), (3, 3)))

    rdd.mapPartitionsWithIndex((index, iter) => {
      Iterator(index.toString + " : " + iter.mkString("|"))
    }).collect.foreach(println)

    //查看当前分区器
    println(rdd.partitions)

    //设置新的分区器
    val rdd2: RDD[(Int, Int)] = rdd.partitionBy(new HashPartitioner(8))
    //查看新设置后的分区器
    println(rdd2.partitions)
  }
}
~~~



### 5.3 Range分区

1. HashPartitioner分区弊端：可能导致每个分区中数据量的不均匀，极端情况下会导致某些分区拥有RDD的全部数据。

2. ```
   RangePartitioner作用：将一定范围内的数映射到某一个分区内，尽量保证每个分区中数据量的均匀，而且分区与分区之间是有序的，一个分区中的元素肯定都是比另一个分区内的元素小或者大，但是分区内的元素是不能保证顺序的。简单的说就是将一定范围内的数映射到某一个分区内
   ```

实现步骤:

1. ```
   先重整个RDD中抽取出样本数据，将样本数据排序，计算出每个分区的最大key值，形成一个Array[KEY]类型的数组变量rangeBounds；
   ```

2. ```
   判断key在rangeBounds中所处的范围，给出该key值在下一个RDD中的分区id下标；该分区器要求RDD中的KEY类型必须是可以排序的
   ```

### 5.4 自定义分区

~~~scala
package cn.xhjava.spark.rdd.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * 自定义分区
  */
object Spark_RDD_partitionBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_RDD_partitionBy")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "c")), 3)
    //分区
    val partitionRDD: RDD[(Int, String)] = rdd.partitionBy(new MyPartitioner(2))

    println(partitionRDD.partitions)
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

## 六. 文件的读取与保存

### 6.1 text

~~~scala
package cn.xhjava.spark.rdd.dataIO

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Text文件的读取与写入
  */
object Spark_DateIO_Text {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_DateIO_Text")
    val sc = new SparkContext(conf)

    //读取txt文本
    val textRDD: RDD[String] = sc.textFile("F:\\MyLearning\\SparkStudy\\in\\test")

    //将读取的数据保存到txt文本
    textRDD.saveAsTextFile("in/test2")
  }
}

~~~



### 6.2 json

~~~scala
package cn.xhjava.spark.rdd.dataIO

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.parsing.json.JSON

/**
  * Json文件的读取
  * 如果JSON文件中每一行就是一个JSON记录，那么可以通过将JSON文件当做文本文件来读取，然后利用相关的JSON库对每一条数据进行JSON解析。
  *
  */
object Spark_DateIO_Json {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_DateIO_Json")
    val sc = new SparkContext(conf)

    //TODO 注意：使用RDD读取JSON文件处理很复杂，同时SparkSQL集成了很好的处理JSON文件的方式，所以应用中多是采用SparkSQL处理JSON文件


    //读取json文本
    val jsonRDD: RDD[String] = sc.textFile("F:\\MyLearning\\SparkStudy\\in\\people.json")

    //解析json数据并且打印
    jsonRDD.map(JSON.parseFull).collect().foreach(println)
  }
}

~~~



### 6.3 sequence

~~~scala
package cn.xhjava.spark.rdd.dataIO

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * SequenceFile文件写入
  * SequenceFile文件是Hadoop用来存储二进制形式的key-value对而设计的一种平面文件(Flat File)
  */
object Spark_DateIO_Squence {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_Dependen_Demo")
    val sc = new SparkContext(conf)

    //生成RDD
    val rdd: RDD[(Int, Int)] = sc.makeRDD(Array((1,2),(3,4),(5,6)))

    //将读取的数据保存到Sequence文本
    rdd.saveAsObjectFile("in/Sequence")
    //读取Sequence 文件打印
    sc.sequenceFile[Int,Int]("in/Sequence").collect().foreach(println)
  }
}

~~~



### 6.4 object

~~~scala
package cn.xhjava.spark.rdd.dataIO

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 对象文件
  * 对象文件是将对象序列化后保存的文件，采用Java的序列化机制。
  * 可以通过objectFile[k,v](path) 函数接收一个路径，读取对象文件，返回对应的 RDD，
  * 也可以通过调用saveAsObjectFile() 实现对对象文件的输出。因为是序列化所以要指定类型
  */
object Spark_DateIO_Object {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_Dependen_Demo")
    val sc = new SparkContext(conf)

    //
    val rdd: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4))

    //将RDD保存为Object文件
    rdd.saveAsObjectFile("in/object")
    sc.objectFile[Int]("in/object").collect().foreach(println)
  }
}

~~~



### 6.5 HDFS

~~~scala
package cn.xhjava.spark.rdd.fileSystemIO

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * HDFS
  */
object Spark_DateIO_Hdfs {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_Dependen_Demo")
    val sc = new SparkContext(conf)

    /**
      * Spark的整个生态系统与Hadoop是完全兼容的,
      * 所以对于Hadoop所支持的文件类型或者数据库类型,Spark也同样支持.
      * 另外,由于Hadoop的API有新旧两个版本,所以Spark为了能够兼容Hadoop所有的版本,也提供了两套创建操作接口.
      * 对于外部存储创建操作而言,hadoopRDD和newHadoopRDD是最为抽象的两个函数接口,主要包含以下四个参数.
      * 1）输入格式(InputFormat): 制定数据输入的类型,如TextInputFormat等,新旧两个版本所引用的版本分别是org.apache.hadoop.mapred.InputFormat和org.apache.hadoop.mapreduce.InputFormat(NewInputFormat)
      * 2）键类型: 指定[K,V]键值对中K的类型
      * 3）值类型: 指定[K,V]键值对中V的类型
      * 4）分区值: 指定由外部存储生成的RDD的partition数量的最小值,如果没有指定,系统会使用默认值defaultMinSplits
      */

    /**
      * 注意:其他创建操作的API接口都是为了方便最终的Spark程序开发者而设置的,是这两个接口的高效实现版本.
      * 例如,对于textFile而言,只有path这个指定文件路径的参数,其他参数在系统内部指定了默认值。
      */

    /**
      * 1.在Hadoop中以压缩形式存储的数据,不需要指定解压方式就能够进行读取,
      * 因为Hadoop本身有一个解压器会根据压缩文件的后缀推断解压算法进行解压.
      * 2.如果用Spark从Hadoop中读取某种类型的数据不知道怎么读取的时候,
      * 上网查找一个使用map-reduce的时候是怎么读取这种这种数据的,
      * 然后再将对应的读取方式改写成上面的hadoopRDD和newAPIHadoopRDD两个类就行了
      */
  }
}

~~~



### 6.6 Mysql

~~~scala
package cn.xhjava.spark.rdd.fileSystemIO

import java.sql.{Connection, DriverManager, PreparedStatement, Statement}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * MySQL
  */
object Spark_DateIO_MySQL {
  def main(args: Array[String]): Unit = {

    val mysql = new MysqlUtil
    mysql.insert
    //mysql.select

  }
}


class MysqlUtil {
  def select = {
    //1.创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JdbcRDD")

    //2.创建SparkContext
    val sc = new SparkContext(sparkConf)

    //3.定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/test"
    val userName = "root"
    val passWd = "root"

    //创建JdbcRDD
    val rdd = new JdbcRDD(sc, () => {
      Class.forName(driver)
      DriverManager.getConnection(url, userName, passWd)
    },
      "select * from rddtable where id>=? AND id < ?;",
      1,
      10,
      1,
      r => (r.getString(1), r.getString(2))
    )

    //打印最后结果
    println(rdd.count())
    rdd.foreach(println)

    sc.stop()

  }



  def insert ={
    //1.创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JdbcRDD")
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/test"
    val userName = "root"
    val passWd = "root"
    //2.创建SparkContext
    val sc = new SparkContext(sparkConf)
    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("gg",11),("hh",22),("kk",33)))
    dataRDD.foreach{
      case(name,size) =>{
        Class.forName(driver)
        val connection: Connection = DriverManager.getConnection(url, userName, passWd)
        val sql = "insert into rddtable (name,size) values(?,?)"
        val statement: PreparedStatement = connection.prepareStatement(sql)
        statement.setString(1,name)
        statement.setInt(2,size)
        statement.executeUpdate()
        statement.close()
      }
    }
  }
}

~~~



### 6.7 Hbase

~~~scala
package cn.xhjava.spark.rdd.fileSystemIO

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{HBaseAdmin, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Hbase
  */
object Spark_DateIO_Hbase {


  //read
  def main(args: Array[String]): Unit = {
    //创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JdbcRDD")

    //创建SparkContext
    val sc = new SparkContext(sparkConf)

    //构建HBase配置信息
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104")
    conf.set(TableInputFormat.INPUT_TABLE, "rddtable")

    //从HBase读取数据形成RDD
    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val count: Long = hbaseRDD.count()
    println(count)

    //对hbaseRDD进行处理
    hbaseRDD.foreach {
      case (_, result) =>
        val key: String = Bytes.toString(result.getRow)
        val name: String = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")))
        val color: String = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("color")))
        println("RowKey:" + key + ",Name:" + name + ",Color:" + color)
    }

    //关闭连接
    sc.stop()
  }

  def write(args: Array[String]) {
    //获取Spark配置信息并创建与spark的连接
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HBaseApp")
    val sc = new SparkContext(sparkConf)

    //创建HBaseConf
    val conf = HBaseConfiguration.create()
    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "fruit_spark")

    //构建Hbase表描述器
    val fruitTable = TableName.valueOf("fruit_spark")
    val tableDescr = new HTableDescriptor(fruitTable)
    tableDescr.addFamily(new HColumnDescriptor("info".getBytes))

    //创建Hbase表
    val admin = new HBaseAdmin(conf)
    if (admin.tableExists(fruitTable)) {
      admin.disableTable(fruitTable)
      admin.deleteTable(fruitTable)
    }
    admin.createTable(tableDescr)

    //定义往Hbase插入数据的方法
    def convert(triple: (Int, String, Int)) = {
      val put = new Put(Bytes.toBytes(triple._1))
      put.addImmutable(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(triple._2))
      put.addImmutable(Bytes.toBytes("info"), Bytes.toBytes("price"), Bytes.toBytes(triple._3))
      (new ImmutableBytesWritable, put)
    }

    //创建一个RDD
    val initialRDD = sc.parallelize(List((1,"apple",11), (2,"banana",12), (3,"pear",13)))

    //将RDD内容写到HBase
    val localData = initialRDD.map(convert)

    localData.saveAsHadoopDataset(jobConf)
  }
}
~~~



## 七. 累加器

### 7.1 累加器的使用

~~~scala
package cn.xhjava.spark.rdd.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 累加器:分布式只读共享变量
  */
object Spark_accumulator_demo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_accumulator_demo")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    //需求:获取rdd内4个值的和
    //val sum: Int = rdd.reduce(_+_)
    //但是上面的代码逻辑太过于复杂,有什么简单的方法呢?
    //javaSE for循环可以吗?
    //    var sum = 0;
    //    rdd.foreach(i => {
    //      sum = sum + i
    //    })
    //println(sum)
    //此时这个sum=0,不是我们想要的结果,为什么么呢?
    /**
      * spark中,driver将rdd两个分区的内的数据分别传入两个exector做计算,假设：exector1:1.2;exector1:3,4;
      * 我们用foreach循环相加的那一步其实只是将两个exector内的数据做sum,现在exector1,2内的数据分别是：3，7
      * 但是为什么最后我们打印出来的结果是0呢？
      * 因为我们只是分别在两个exectore内计算,最终的结果并没有返回给driver,所以sum 还是我们之前的0;
      */

    //TODO 使用累加器共享变量:
    //1.创建累加器对象
    val accumulator: LongAccumulator = sc.longAccumulator
    var sum = 0
    rdd.foreach{
      case i =>{
        //2.使用累加器累加结果
        accumulator.add(i)
      }
    }
    //3.打印
    println(accumulator.value)
  }

}
~~~



### 7.2 自定义累加器

~~~scala
package cn.xhjava.spark.rdd.accumulator

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 自定义累加器
  */
object Spark_accumulator_UDDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_accumulator_demo")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("spark", "scala", "sparksql", "sparkRDD", "hello"))
    //TODO 1.创建自定义累加器
    val accumulator = new MyAccumulator
    //TODO 2.注册累加器
    sc.register(accumulator)

    rdd.foreach{
      case word =>{
        accumulator.add(word)
      }
    }

    println(accumulator.value)

  }

}

/**
  * 自定义累加器
  * 继承AccumulatorV2,并实现其方法
  */
class MyAccumulator extends AccumulatorV2[String, util.ArrayList[String]] {

  private val arrayList = new util.ArrayList[String]()

  //当前对象是否为初始化
  override def isZero: Boolean = {
    arrayList.isEmpty
  }

  //复制累加器对象
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    new MyAccumulator()
  }

  override def reset(): Unit = {
    arrayList.clear()
  }

  //向累加器中增加数据
  override def add(v: String): Unit = {
    if (v.contains("s")) {
      arrayList.add(v)
    }
  }

  //合并两个累加器的值
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    arrayList.addAll(other.value)
  }


  //获取累加器的结果
  override def value: util.ArrayList[String] = {
    arrayList
  }
}

~~~

## 八. 广播变量

~~~scala
package cn.xhjava.spark.rdd.boradcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 广播变量用来高效分发较大的对象。
  * 向所有工作节点发送一个较大的只读值，以供一个或多个Spark操作使用。
  * 比如，如果你的应用需要向所有节点发送一个较大的只读查询表，
  * 甚至是机器学习算法中的一个很大的特征向量， 广播变量用起来都很顺手。
  * 在多个并行操作中使用同一个变量，但是 Spark会为每个任务分别发送。
  */
object Spark_boradcast_Demo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_boradcast_Demo")
    val sc = new SparkContext(conf)

    val rdd: RDD[(Int, Int)] = sc.makeRDD(List((1, 1), (1, 2), (1, 3)))
    //    val rdd2: RDD[(Int, Int)] = sc.makeRDD(List((1, 1), (2, 2), (3, 3)))
    //    val joinRDD: RDD[(Int, (Int, Int))] = rdd.join(rdd2)
    //    joinRDD.foreach(println)
    //现在因为join产生了shuffer操作,太慢了，需要另外的方法
    val list = List((1, 1), (2, 2), (3, 3))

    //使用广播变量减少数据的传输
    val broadcast: Broadcast[List[(Int, Int)]] = sc.broadcast(list)

    val result: RDD[(Int, (Int, Any))] = rdd.map {
      case (key, value) => {
        var v2: Any = null
        for (t <- broadcast.value) {
          if (key == t._1) {
            v2 = t._2
          }
        }
        (key, (value, v2))
      }
    }
    result.foreach(println)

  }
}
~~~





