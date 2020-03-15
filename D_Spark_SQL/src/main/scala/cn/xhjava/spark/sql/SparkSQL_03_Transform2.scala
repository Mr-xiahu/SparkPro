package cn.xhjava.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * RDD--->DS--->RDD--->打印
  */
object SparkSQL_03_Transform2 {
  def main(args: Array[String]): Unit = {
    //初始化SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("SparkSQL_03_Transform2")
      .config("spark.some.config.option", "some-value")
      .master("local[3]")
      .getOrCreate()

    //进行转换之前,需要隐式转换规则
    //这里的spark不是包名的含义,而是SparkSession对象名字
    import spark.implicits._

    //创建RDD
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 21), (2, "lisi", 22), (3, "wangwu", 23)))

    //转换为RS
    val userRDD: RDD[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }
    val ds: Dataset[User] = userRDD.toDS()
    //转换为RDD

    val rdd2: RDD[User] = ds.rdd
    //打印
    rdd2.foreach(println)
    //释放资源
    spark.stop()
  }

  //增加一个样例类
  case class User(id: Int, name: String, age: Int)

}


