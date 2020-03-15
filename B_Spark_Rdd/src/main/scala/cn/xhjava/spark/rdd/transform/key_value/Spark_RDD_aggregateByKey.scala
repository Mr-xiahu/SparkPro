package cn.xhjava.spark.rdd.transform.key_value

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
    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_RDD_aggregateByKey")
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


