package cn.xjhava.spark.structurestream

import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * @author Xiahu
  * @create 2020/7/7
  */

/**
  * StructureStream WordCount
  */
object A_WordCount2 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[3]").getOrCreate()
    val dataFrame: DataFrame = spark.readStream.format("socket")
      .option("host", "node2")
      .option("port", "8999")
      .load()

    import spark.implicits._

    //输出
    val wordCount: DataFrame = dataFrame.as[String]
      .flatMap(_.split(" "))
      .groupBy("value").count() //这里聚合会比较慢

    val result: StreamingQuery = wordCount
      .writeStream
      .format("console")
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("2 seconds")) //间隔多长时间执行一次
      .start()

    //持续运行
    result.awaitTermination()


  }
}
