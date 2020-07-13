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
object A_WordCount {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[3]").getOrCreate()

    import spark.implicits._
    val dataFrame: DataFrame = spark.readStream.format("socket")
      .option("host", "node2")
      .option("port", "8999")
      .load()
    //输出
    val result: StreamingQuery = dataFrame.writeStream.format("console")
      .outputMode("append") //complete append update
      .start()

    //持续运行
    result.awaitTermination()

    /**
      * complete:全部輸出,必須有聚合
      * append:追加,只输出那些将来永远不可能更新的数据.
      *     没有聚合时,append 与 update一样
      *     有聚合时,一定要存在watermark才能使用append
      * update:只输出变化的部分
      */

  }
}
