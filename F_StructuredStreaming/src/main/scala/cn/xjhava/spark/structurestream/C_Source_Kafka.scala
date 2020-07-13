package cn.xjhava.spark.structurestream

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  *
  * @author Xiahu
  * @create 2020/7/7
  */

/**
  * StructureStream 数据源 kafka
  * http://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
  */
object C_Source_Kafka {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[3]").getOrCreate()
    import spark.implicits._
    val df: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node3:9020")
      .option("subscribe", "xiahu2")
      .load()

//    val kafkaData: Dataset[(String, String)] = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
//      .as[(String, String)]


    val result: StreamingQuery = df
      .writeStream
      .format("console")
      .outputMode("append")
      .start()

    //持续运行
    result.awaitTermination()


  }
}
