package cn.xhjava.spark.rdd.transform.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 针对于(K,V)形式的类型只对V进行操作
  */
object Spark_RDD_mapValue {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_RDD_mapValue")
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


