package cn.xhjava.spark.sql.datasource

import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * 当数据源格式不是parquet格式文件时，需要手动指定数据源的格式。
  * 数据源格式需要指定全名（例如：org.apache.spark.sql.parquet），
  * 如果数据源格式为内置格式，
  * 则只需要指定简称定json, parquet, jdbc, orc, libsvm, csv, text来指定数据的格式
  */
object Spark_datasource_B_othertype {
  def main(args: Array[String]): Unit = {
    //初始化SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("Spark_datasource_B_othertype")
      .config("spark.some.config.option", "some-value")
      .master("local[3]")
      .getOrCreate()

    //下面以json文件为例
    val df: DataFrame = spark.read.format("json").load("in/people.json")
    df.show(100)

    //另外,保存的格式也可以手动指定
    df.write.format("parquet").save("")
  }
}
