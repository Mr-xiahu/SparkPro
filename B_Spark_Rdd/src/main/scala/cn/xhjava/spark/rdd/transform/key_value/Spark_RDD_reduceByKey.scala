package cn.xhjava.spark.rdd.transform.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * reduceByKey(func, [numTasks])
  * 在一个(K,V)的RDD上调用，返回一个(K,V)的RDD，
  * 使用指定的reduce函数，将相同key的值聚合到一起，reduce任务的个数可以通过numTasks来设置
  */
object Spark_RDD_reduceByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_RDD_reduceByKey")
    val sc = new SparkContext(conf)
    //创建RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("female", 1), ("male", 5), ("female", 5), ("male", 2)))
    //reduceByKey,将key相同的直接进行reduce计算
    val reduceByKeyRDD: RDD[(String, Int)] = rdd.reduceByKey((x, y) => x * y)
    //打印结果:
//    (male,10)
//    (female,5)

    reduceByKeyRDD.collect().foreach(println)


  }


  //reduceByKey和groupByKey的区别
  //reduceByKey：按照key进行聚合，在shuffle之前有combine（预聚合）操作，返回结果是RDD[k,v].
  //groupByKey：按照key进行分组，直接进行shuffle。
  //性能推荐：reduceByKey优先使用,需要注意是否影响业务实现
}


