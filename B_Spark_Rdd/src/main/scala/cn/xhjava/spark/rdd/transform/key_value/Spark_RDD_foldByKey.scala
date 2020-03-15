package cn.xhjava.spark.rdd.transform.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * aggregateByKey的简化操作
  */
object Spark_RDD_foldByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_RDD_foldByKey")
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


