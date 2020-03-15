package cn.xhjava.spark.rdd.transform.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 对相同K，把V合并成一个集合。
  */
object Spark_RDD_combineByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_RDD_combineByKey")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)), 2)

    /**
      * 参数描述:
      * createCombiner:
      * combineByKey() 会遍历分区中的所有元素，因此每个元素的键要么还没有遇到过，要么就和之前的某个元素的键相同。
      * 如果这是一个新的元素,combineByKey()会使用一个叫作createCombiner()的函数来创建那个键对应的累加器的初始值
      * mergeValue:
      * 如果这是一个在处理当前分区之前已经遇到的键，它会使用mergeValue()方法将该键的累加器对应的当前值与这个新的值进行合并
      * mergeCombiners:
      * 由于每个分区都是独立处理的， 因此对于同一个键可以有多个累加器。
      * 如果有两个或者更多的分区都有对应同一个键的累加器， 就需要使用用户提供的 mergeCombiners() 方法将各个分区的结果进行合并。
      */

    //需求:
    //创建一个pairRDD，根据key计算每种key的均值(先计算每个key出现的次数以及可以对应值的总和，再相除得到结果）
    val combineByKeyRDD: RDD[(String, (Int, Int))] = rdd.combineByKey(
      (_, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))

    //(_, 1):将每个分区的每个(Key,value)中的value进行map操作,将key对应的value转成tuple
    // (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1):元组的第一位与V相加，第二位自增1
    //(acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))：元组的第一位与第二位分别累加

    val resultRDD: RDD[(String, Double)] = combineByKeyRDD.map {
      case (key, value) => (key, value._1 / value._2.toDouble)
    }


    resultRDD.collect().foreach(println)
    //打印结果:
    //    (b,95.33333333333333)
    //    (a,91.33333333333333)


  }
}


