package cn.xjhava.spark.structurestream

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * @author Xiahu
  * @create 2020/7/7
  */

/**
  * StructureStream WordCount
  */
object A_WordCount3 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[3]").getOrCreate()
    val dataFrame: DataFrame = spark.readStream.format("socket")
      .option("host", "node2")
      .option("port", "8999")
      .load()

    import spark.implicits._

    //输出
    dataFrame.as[String]
      .flatMap(_.split(" ")).createOrReplaceTempView("w")
    val wordCount: DataFrame = spark.sql(
      """
        |select
        |*,count(*) as size
        |from
        |w
        |group by value
      """.stripMargin)

    val result: StreamingQuery = wordCount
      .writeStream
      .format("console")
      .outputMode("update")
      .start()

    //持续运行
    result.awaitTermination()


  }
}
