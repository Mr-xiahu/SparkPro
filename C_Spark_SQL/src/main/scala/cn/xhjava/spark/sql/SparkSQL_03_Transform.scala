package cn.xhjava.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * RDD-->DF-->DS-->DF-->RDD-->打印
  */
object SparkSQL_03_Transform {
  def main(args: Array[String]): Unit = {
    //初始化SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("SparkSQL_03_Transform")
      .config("spark.some.config.option", "some-value")
      .master("local[3]")
      .getOrCreate()



    //创建RDD
    val userRDD: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 21), (2, "lisi", 22), (3, "wangwu", 23)))
    //转换为DF
    //进行转换之前,需要隐式转换规则
    //这里的spark不是包名的含义,而是SparkSession对象名字
    import spark.implicits._
    val df: DataFrame = userRDD.toDF("id", "name", "age")

    //转换为DS
    val ds: Dataset[User] = df.as[User]

    //转换为DF
    val df2: DataFrame = ds.toDF()

    //转换为RDD
    val rdd: RDD[Row] = df2.rdd

    //打印
    rdd.foreach(row => {
      //与JDBC ResultSet一样
      println(row.getString(1))
    })
    //释放资源
    spark.stop()
  }

  //增加一个样例类
  case class User(id: Int, name: String, age: Int)

}


