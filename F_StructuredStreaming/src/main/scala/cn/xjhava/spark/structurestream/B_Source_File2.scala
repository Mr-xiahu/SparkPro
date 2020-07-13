package cn.xjhava.spark.structurestream

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * @author Xiahu
  * @create 2020/7/7
  */

/**
  * StructureStream 数据源 fileSource 自动分区
  */
object B_Source_File2 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[3]").getOrCreate()
    val userSchema = StructType(Array(StructField("name",StringType),StructField("age",IntegerType),StructField("sex",StringType)))
    val dataFrame: DataFrame = spark.readStream.format("csv")
      .schema(userSchema)
      .load("D:\\git\\study\\SparkStudy\\F_StructuredStreaming\\src\\main\\resources\\input2")
    //dataFrame.show(100)



    val result: StreamingQuery = dataFrame
      .writeStream
      .format("console")
      .outputMode("append")
      .start()

    //持续运行
    result.awaitTermination()


  }
}
