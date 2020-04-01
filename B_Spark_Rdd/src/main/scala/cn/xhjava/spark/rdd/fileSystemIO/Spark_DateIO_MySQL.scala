package cn.xhjava.spark.rdd.fileSystemIO

import java.sql.{Connection, DriverManager, PreparedStatement, Statement}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * MySQL
  */
object Spark_DateIO_MySQL {
  def main(args: Array[String]): Unit = {

    val mysql = new MysqlUtil
    mysql.insert
    //mysql.select

  }
}


class MysqlUtil {
  def select = {
    //1.创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JdbcRDD")

    //2.创建SparkContext
    val sc = new SparkContext(sparkConf)

    //3.定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/test"
    val userName = "root"
    val passWd = "root"

    //创建JdbcRDD
    val rdd = new JdbcRDD(sc, () => {
      Class.forName(driver)
      DriverManager.getConnection(url, userName, passWd)
    },
      "select * from rddtable where id>=? AND id < ?;",
      1,
      10,
      1,
      r => (r.getString(1), r.getString(2))
    )

    //打印最后结果
    println(rdd.count())
    rdd.foreach(println)

    sc.stop()

  }



  def insert ={
    //1.创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JdbcRDD")
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/test"
    val userName = "root"
    val passWd = "root"
    //2.创建SparkContext
    val sc = new SparkContext(sparkConf)
    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("gg",11),("hh",22),("kk",33)))
    dataRDD.foreach{
      case(name,size) =>{
        Class.forName(driver)
        val connection: Connection = DriverManager.getConnection(url, userName, passWd)
        val sql = "insert into rddtable (name,size) values(?,?)"
        val statement: PreparedStatement = connection.prepareStatement(sql)
        statement.setString(1,name)
        statement.setInt(2,size)
        statement.executeUpdate()
        statement.close()
      }
    }
  }
}
