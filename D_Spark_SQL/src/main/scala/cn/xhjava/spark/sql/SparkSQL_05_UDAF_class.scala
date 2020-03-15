package cn.xhjava.spark.sql

import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql._

/**
  * 用户自定义聚合函数(强类型)
  */
object SparkSQL_05_UDAF_class {
  def main(args: Array[String]): Unit = {
    //初始化SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("SparkSQL_05_UDAF_class")
      .config("spark.some.config.option", "some-value")
      .master("local[3]")
      .getOrCreate()
    //进行转换之前,需要隐式转换规则
    //这里的spark不是包名的含义,而是SparkSession对象名字
    import spark.implicits._

    //1.创建自定义聚合函数
    val udaf = new MyAgeAvgFunction2
    //2.将聚合函数转换为查询列
    val avgCol: TypedColumn[UserBean, Double] = udaf.toColumn.name("avgAge")
    //3.读取文件---->DF
    val peopleDF: DataFrame = spark.read.json("in/people.json")
    //DF--->DS
    val ds: Dataset[UserBean] = peopleDF.as[UserBean]
    //应用函数
    ds.select(avgCol).show()


    //释放资源
    spark.stop()
  }
}

case class UserBean(name: String, age: BigInt)

case class AvgBuffer(var sum: BigInt, var count: Int)


//申明用户自定义聚合函数(强类型)
//1.继承Aggregator[in,buffer,out]类
//2.实现其方法

class MyAgeAvgFunction2 extends Aggregator[UserBean, AvgBuffer, Double] {
  //初始化
  override def zero: AvgBuffer = {
    AvgBuffer(0, 0)
  }

  //聚合数据
  override def reduce(b: AvgBuffer, a: UserBean): AvgBuffer = {
    b.sum = b.sum + a.age
    b.count = b.count + 1
    b
  }

  //缓冲区的合并操作
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }

  //最终的计算
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count
  }


  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}


