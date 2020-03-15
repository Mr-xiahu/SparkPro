package cn.xhjava.spark.rdd.transform.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 在一个(K,V)的RDD上调用，K必须实现Ordered接口，返回一个按照key进行排序的(K,V)的RDD
  */
object Spark_RDD_sortByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_RDD_sortByKey")
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


