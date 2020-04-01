package cn.xhjava.spark.rdd.boradcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 广播变量用来高效分发较大的对象。
  * 向所有工作节点发送一个较大的只读值，以供一个或多个Spark操作使用。
  * 比如，如果你的应用需要向所有节点发送一个较大的只读查询表，
  * 甚至是机器学习算法中的一个很大的特征向量， 广播变量用起来都很顺手。
  * 在多个并行操作中使用同一个变量，但是 Spark会为每个任务分别发送。
  */
object Spark_boradcast_Demo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Spark_boradcast_Demo")
    val sc = new SparkContext(conf)

    val rdd: RDD[(Int, Int)] = sc.makeRDD(List((1, 1), (1, 2), (1, 3)))
    //    val rdd2: RDD[(Int, Int)] = sc.makeRDD(List((1, 1), (2, 2), (3, 3)))
    //    val joinRDD: RDD[(Int, (Int, Int))] = rdd.join(rdd2)
    //    joinRDD.foreach(println)
    //现在因为join产生了shuffer操作,太慢了，需要另外的方法
    val list = List((1, 1), (2, 2), (3, 3))

    //使用广播变量减少数据的传输
    val broadcast: Broadcast[List[(Int, Int)]] = sc.broadcast(list)

    val result: RDD[(Int, (Int, Any))] = rdd.map {
      case (key, value) => {
        var v2: Any = null
        for (t <- broadcast.value) {
          if (key == t._1) {
            v2 = t._2
          }
        }
        (key, (value, v2))
      }
    }
    result.foreach(println)

  }

}
