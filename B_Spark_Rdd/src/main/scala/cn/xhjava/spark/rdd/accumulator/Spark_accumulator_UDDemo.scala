package cn.xhjava.spark.rdd.accumulator

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 自定义累加器
  */
object Spark_accumulator_UDDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_accumulator_demo")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("spark", "scala", "sparksql", "sparkRDD", "hello"))
    //TODO 1.创建自定义累加器
    val accumulator = new MyAccumulator
    //TODO 2.注册累加器
    sc.register(accumulator)

    rdd.foreach{
      case word =>{
        accumulator.add(word)
      }
    }

    println(accumulator.value)

  }

}

/**
  * 自定义累加器
  * 继承AccumulatorV2,并实现其方法
  */
class MyAccumulator extends AccumulatorV2[String, util.ArrayList[String]] {

  private val arrayList = new util.ArrayList[String]()

  //当前对象是否为初始化
  override def isZero: Boolean = {
    arrayList.isEmpty
  }

  //复制累加器对象
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    new MyAccumulator()
  }

  override def reset(): Unit = {
    arrayList.clear()
  }

  //向累加器中增加数据
  override def add(v: String): Unit = {
    if (v.contains("s")) {
      arrayList.add(v)
    }
  }

  //合并两个累加器的值
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    arrayList.addAll(other.value)
  }


  //获取累加器的结果
  override def value: util.ArrayList[String] = {
    arrayList
  }
}
