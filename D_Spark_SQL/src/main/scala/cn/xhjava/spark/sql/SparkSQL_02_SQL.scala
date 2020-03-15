package cn.xhjava.spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL_02_SQL {
  def main(args: Array[String]): Unit = {
    //初始化SparkSession
    val session: SparkSession = SparkSession.builder()
      .appName("SparkSQL_02_SQL")
      .config("spark.some.config.option", "some-value")
      .master("local[3]")
      .getOrCreate()


    val peopleDF: DataFrame = session.read.json("in/people.json")

    //将DataFrame转换为一张表
    peopleDF.createOrReplaceTempView("people")

    session.sql("select * from people").show()

    //释放资源
    session.stop()
  }

}
