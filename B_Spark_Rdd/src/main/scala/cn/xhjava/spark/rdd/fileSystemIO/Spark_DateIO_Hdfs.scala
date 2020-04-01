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
