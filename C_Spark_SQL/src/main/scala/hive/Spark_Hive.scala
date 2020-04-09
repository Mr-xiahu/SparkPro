package hive

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


/**
  *
  */
object Spark_Hive {
  def main(args: Array[String]): Unit = {
    //初始化SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("Spark_datasource_FileSave")
      .config("spark.some.config.option", "some-value")
      .master("local[3]")
      .getOrCreate()

    /**
      * 若要把Spark SQL连接到一个部署好的Hive上，
      * 你必须把hive-site.xml复制到 Spark的配置文件目录中($SPARK_HOME/conf)。
      * 即使没有部署好Hive，Spark SQL也可以运行。
      * 需要注意的是，如果你没有部署好Hive，Spark SQL会在当前的工作目录中创建出自己的Hive 元数据仓库，
      * 叫作 metastore_db。
      * 此外，如果你尝试使用 HiveQL 中的 CREATE TABLE (并非 CREATE EXTERNAL TABLE)语句来创建表，
      * 这些表会被放在你默认的文件系统中的 /user/hive/warehouse
      * 目录中(如果你的 classpath 中有配好的 hdfs-site.xml，默认的文件系统就是 HDFS，否则就是本地文件系统)。
      */

    /**
      * 注意：如果你使用的是内部的Hive，
      * 在Spark2.0之后，spark.sql.warehouse.dir用于指定数据仓库的地址，
      * 如果你需要是用HDFS作为路径，那么需要将core-site.xml和hdfs-site.xml 加入到Spark conf目录，
      * 否则只会创建master节点上的warehouse目录，查询时会出现文件找不到的问题，这是需要使用HDFS，
      * 则需要将metastore删除，重启集群。
      */
    //TODO 代码在公司电脑，记得补充
  }
}
