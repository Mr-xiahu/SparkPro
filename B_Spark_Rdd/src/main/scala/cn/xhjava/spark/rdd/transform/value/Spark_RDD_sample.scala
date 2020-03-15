package cn.xhjava.spark.rdd.transform.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 随机抽样
  */
object Spark_RDD_sample {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_RDD_sample")
    val sc = new SparkContext(conf)

    val listRdd: RDD[Int] = sc.makeRDD(1 to 10)

    // withReplacement:boolean类型.抽出的数据是否放回;true/false :放回/不放回
    // fraction:Double类型,内容只能位于[0-1]之间.类似于提供一个标准.
    // seed:随机数生成器的种子

    //从指定的数据集合中进行抽样处理,根据不同的算法进行抽象
    // 放回抽样
    //    val sampleRdd: RDD[Int] = listRdd.sample(false,1,1)
    //不放回抽样
    val sampleRdd: RDD[Int] = listRdd.sample(true, 5, 2)
    //收集并且打印
    sampleRdd.collect().foreach(println)
  }
}
