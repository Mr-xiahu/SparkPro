package cn.xhjava.spark.rdd.transform.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * groupByKe对每个key进行操作，只生成一个sequence(集合)
  */
object Spark_RDD_groupByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_RDD_groupByKey")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[String] = sc.makeRDD(Array("one", "two", "two", "three", "three", "three"))
    //使用map转换结构
    val mapRDD: RDD[(String, Int)] = rdd.map((_, 1))
    //groupByKey
    val groupByKeyRdd: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
    //打印结果:
    //(two,CompactBuffer(1, 1))
    //(one,CompactBuffer(1))
    //(three,CompactBuffer(1, 1, 1))
    //计算相同key分组后的累加值
    val result: RDD[(String, Int)] = groupByKeyRdd.map(t => (t._1, t._2.sum))
    //打印结果:
    //    (two,2)
    //    (one,1)
    //    (three,3)

    result.collect().foreach(println)


  }
}


