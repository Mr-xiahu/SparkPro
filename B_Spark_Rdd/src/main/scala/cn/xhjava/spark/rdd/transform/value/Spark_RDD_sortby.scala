package cn.xhjava.spark.rdd.transform.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 使用func先对数据进行处理，按照处理后的数据比较结果排序，默认为正序。
  */
object Spark_RDD_sortby {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_RDD_sortby")
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
