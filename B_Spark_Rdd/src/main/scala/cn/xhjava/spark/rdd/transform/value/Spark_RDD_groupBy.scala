package cn.xhjava.spark.rdd.transform.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 分组，按照传入函数的返回值进行分组。将相同的key对应的值放入一个迭代器。
  */
object Spark_RDD_groupBy {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_RDD_groupBy")
    val sc = new SparkContext(conf)

    //生成数据，按照指定规则进行分区
    val listRdd: RDD[Int] = sc.makeRDD(1 to 10)

    // 分组后的数据形成了对偶元组(K-V),K表示分组的key,V表示分组后的数据元组
    val groupByRdd: RDD[(Int, Iterable[Int])] = listRdd.groupBy(_ % 2)

    //收集并且打印
    groupByRdd.collect().foreach(println)
  }
}
