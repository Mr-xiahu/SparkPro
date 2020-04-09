package cn.xhjava.spark.sql.json

import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * Spark SQL 能够自动推测 JSON数据集的结构，并将它加载为一个Dataset[Row].
  * 可以通过SparkSession.read.json()去加载一个 一个JSON 文件
  * 注意：这个JSON文件不是一个传统的JSON文件，每一行都得是一个JSON串。
  */
object Spark_Json {
  def main(args: Array[String]): Unit = {
    //初始化SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("Spark_datasource_A_parquet")
      .config("spark.some.config.option", "some-value")
      .master("local[3]")
      .getOrCreate()

    val df: DataFrame = spark.read.json("in/people.json")
    df.show(100)
    df.write.save("")
  }
}
