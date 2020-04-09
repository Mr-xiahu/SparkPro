package cn.xhjava.spark.sql.datasource

import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * Spark SQL的默认数据源为Parquet格式。
  * 数据源为Parquet文件时，Spark SQL可以方便的执行所有的操作。
  * 修改配置项spark.sql.sources.default，可修改默认数据源格式。
  */
object Spark_datasource_A_parquet {
  def main(args: Array[String]): Unit = {
    //初始化SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("Spark_datasource_A_parquet")
      .config("spark.some.config.option", "some-value")
      .master("local[3]")
      .getOrCreate()

    val df: DataFrame = spark.read.parquet("in/users.parquet")
    df.show(100)
    df.write.save("")
  }
}
