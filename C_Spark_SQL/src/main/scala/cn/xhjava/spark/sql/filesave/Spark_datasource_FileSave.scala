package cn.xhjava.spark.sql.filesave

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


/**
  * 可以采用SaveMode执行存储操作，SaveMode定义了对数据的处理模式。
  * 需要注意的是，这些保存模式不使用任何锁定，不是原子操作。
  * 此外，当使用Overwrite方式执行时，在输出新数据之前原数据就已经被删除。SaveMode详细介绍如下表：
  */
object Spark_datasource_FileSave {
  def main(args: Array[String]): Unit = {
    //初始化SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("Spark_datasource_FileSave")
      .config("spark.some.config.option", "some-value")
      .master("local[3]")
      .getOrCreate()

    //下面以json文件为例
    val df: DataFrame = spark.read.format("json").load("in/people.json")

    df.write.format("").mode(SaveMode.Append).save()
    SaveMode.ErrorIfExists //如果文件存在，则报错
    SaveMode.Append //追加
    SaveMode.Overwrite //覆盖写入
    SaveMode.Ignore //数据存在则忽略

  }
}
