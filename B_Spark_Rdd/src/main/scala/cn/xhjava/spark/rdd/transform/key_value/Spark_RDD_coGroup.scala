package cn.xhjava.spark.rdd.transform.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 在类型为(K,V)和(K,W)的RDD上调用，返回一个(K,(Iterable<V>,Iterable<W>))类型的RDD
  */
object Spark_RDD_coGroup {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_RDD_coGroup")
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


