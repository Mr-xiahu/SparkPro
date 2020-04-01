package cn.xhjava.spark.rdd.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 累加器:分布式只读共享变量
  */
object Spark_accumulator_demo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_accumulator_demo")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    //需求:获取rdd内4个值的和
    //val sum: Int = rdd.reduce(_+_)
    //但是上面的代码逻辑太过于复杂,有什么简单的方法呢?
    //javaSE for循环可以吗?
    //    var sum = 0;
    //    rdd.foreach(i => {
    //      sum = sum + i
    //    })
    //println(sum)
    //此时这个sum=0,不是我们想要的结果,为什么么呢?
    /**
      * spark中,driver将rdd两个分区的内的数据分别传入两个exector做计算,假设：exector1:1.2;exector1:3,4;
      * 我们用foreach循环相加的那一步其实只是将两个exector内的数据做sum,现在exector1,2内的数据分别是：3，7
      * 但是为什么最后我们打印出来的结果是0呢？
      * 因为我们只是分别在两个exectore内计算,最终的结果并没有返回给driver,所以sum 还是我们之前的0;
      */

    //TODO 使用累加器共享变量:
    //1.创建累加器对象
    val accumulator: LongAccumulator = sc.longAccumulator
    var sum = 0
    rdd.foreach{
      case i =>{
        //2.使用累加器累加结果
        accumulator.add(i)
      }
    }
    //3.打印
    println(accumulator.value)
  }

}
