package cn.xhjava.spark.sql.datasource

import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * 除此之外，可以直接运行SQL在文件上
  */
object Spark_datasource_C_sql {
  def main(args: Array[String]): Unit = {
    //初始化SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("Spark_datasource_C_sql")
      .config("spark.some.config.option", "some-value")
      .master("local[3]")
      .getOrCreate()

    //下面以json文件为例
    val sqlDF = spark.sql("SELECT * FROM parquet.`hdfs://hadoop102:9000/namesAndAges.parquet`")
    sqlDF.show()


    //另外,保存的格式也可以手动指定
    //df.write.format("parquet").save("")
  }
}
