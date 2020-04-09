package jdbc

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * Spark SQL可以通过JDBC从关系型数据库中读取数据的方式创建DataFrame，
  * 通过对DataFrame一系列的计算后，还可以将数据再写回关系型数据库中。
  *
  * 注意:需要将相关的数据库驱动放到spark的类路径下。
  */
object Spark_jdbc {
  val spark: SparkSession = SparkSession.builder()
    .appName("Spark_jdbc")
    .config("spark.some.config.option", "some-value")
    .master("local[3]")
    .getOrCreate()


  //sparkshell中使用
  def readDataFromSparkShell(): Unit = {
    //从spark-shell 加载数据
    spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/rdd")
      .option("dbtable", "rddtable")
      .option("user", "root")
      .option("password", "000000")
      .load()

  }

  //scala代码使用
  def readDataFromCode(): Unit = {
    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "000000")
    val jdbcDF2 = spark.read
      .jdbc("jdbc:mysql://hadoop102:3306/rdd", "rddtable", connectionProperties)
  }

  //sparkshell中使用
  def writeData(): Unit = {
    val df: DataFrame = spark.read.json("in/people.json")
    df.write
      .format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/rdd")
      .option("dbtable", "dftable")
      .option("user", "root")
      .option("password", "000000")
      .save()


  }

  //scala代码使用
  def writeData2(): Unit = {
    val df: DataFrame = spark.read.json("in/people.json")
    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "000000")
    df.write
      .jdbc("jdbc:mysql://hadoop102:3306/rdd", "db", connectionProperties)


  }
}
