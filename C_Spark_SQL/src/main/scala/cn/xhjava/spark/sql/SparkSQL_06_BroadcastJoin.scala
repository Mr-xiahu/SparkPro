package cn.xhjava.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.broadcast

object SparkSQL_06_BroadcastJoin {
  def main(args: Array[String]): Unit = {
    val sql = "select /*+ BROADCAST(b) */  a.id,a.xjrv,a.jify,b.bfqpf,b.qour from sparksql_join_test a left join sparksql_join_test2  b on a.id = b.fk_id"
    val start = System.currentTimeMillis()
    val conf = new SparkConf().setAppName("Test Spark SQL Speed")
    conf.set("spark.sql.warehouse.dir", "hdfs://master:8020/user/hive/warehouse")
    conf.set("spark.sql.autoBroadcastJoinThreshold", "1073741824")
    val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    val df: DataFrame = spark.sql("select * from xh.sparksql_join_test").toDF()
    val df2: DataFrame = spark.sql("select * from xh.sparksql_join_test2").toDF()
    df2.cache()
    df.createOrReplaceTempView("sparksql_join_test")
    df2.createOrReplaceTempView("sparksql_join_test2")
    broadcast(spark.sql(sql)).show(20)
  }

  def test3(): Unit = {
    val conf = new SparkConf().setAppName("Test Spark SQL Speed")
    conf.set("spark.sql.warehouse.dir", "hdfs://master:8020/user/hive/warehouse")
    conf.set("spark.sql.autoBroadcastJoinThreshold", "1073741824")
    val spark = SparkSession.builder()
      .config(conf)
      .master("local[5]")
      .enableHiveSupport()
      .getOrCreate()

    val small: DataFrame = spark.sql("select * from xh.mdm_all_tmp").toDF()
    small.createOrReplaceTempView("mdm_all_tmp")
    small.cache()
    val big: DataFrame = spark.sql("select * from xh.ab_ip_feelist").toDF()
    big.createOrReplaceTempView("ab_ip_feelist")
    big.join(small.hint("broadcast"), Seq("hospitalno"), "left").show(10)


  }
}
