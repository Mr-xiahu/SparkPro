package cn.xjhava.spark.structurestream

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * @author Xiahu
  * @create 2020/7/7
  */

/**
  * StructureStream 数据源 Rate 速率
  *
  */
object D_Source_Rate {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[3]").getOrCreate()
    val df: DataFrame = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", "10")
      .option("rampUpTime", "1")
      .option("numPartitions", "3")
      .load()


    val result: StreamingQuery = df
      .writeStream
      .format("console")
      .outputMode("append")
      .start()

    //持续运行
    result.awaitTermination()


  }
}
