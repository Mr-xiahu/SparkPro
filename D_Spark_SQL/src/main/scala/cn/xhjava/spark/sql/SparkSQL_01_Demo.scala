package cn.xhjava.spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL_01_Demo {
  def main(args: Array[String]): Unit = {
    //初始化SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("SparkSQL_01_Demo")
      .config("spark.some.config.option", "some-value")
      .master("local[3]")
      .getOrCreate()

    val peopleDF: DataFrame = spark.read.json("in/people.json")

    peopleDF.show()
    //释放资源
    spark.stop()
  }

}
