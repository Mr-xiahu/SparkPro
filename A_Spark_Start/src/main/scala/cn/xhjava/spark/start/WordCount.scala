package cn.xhjava.spark.start

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //使用IDEA开发工具完成WordCount

    //local 模式
    //创建SparkConf对象
    val conf = new SparkConf().setMaster("local[3]").setAppName("Word Count")
    val sc = new SparkContext(conf)
    //读取文件内容
    val lines = sc.textFile("F:\\MyLearning\\SparkStudy\\A_Spark_Start\\src\\main\\resources\\word.txt")
    //扁平化操作
    val word: RDD[String] = lines.flatMap(_.split(" "))
    //将word做分组
    val words: RDD[(String, Int)] = word.map((_, 1))
    //聚合
    val unit: RDD[(String, Int)] = words.reduceByKey(_ + _)
    //收集并且打印
    val result: Array[(String, Int)] = unit.collect()
    result.foreach(println)
  }
}
