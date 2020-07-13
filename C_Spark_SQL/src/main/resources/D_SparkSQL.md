# C_SparkSQL

## 一. 概述

### 1. 什么是SparkSQL

Spark SQL是Spark用来处理结构化数据的一个模块，它提供了2个编程抽象：

1. **DataFrame**
2. **DataSet**

并且作为分布式SQL查询引擎的作用。

~~~
众所周知的Hive,它是将Hive SQL转换成MapReduce然后提交到集群上执行，大大简化了编写MapReduc的程序的复杂性，由于MapReduce这种计算模型执行效率比较慢。
所以Spark SQL的应运而生，它是将Spark SQL转换成RDD，然后提交到集群执行，执行效率非常快！
~~~

### 2. SparkSQL的特点

1. 易整合
2. 统一的数据访问方式
3. 兼容Hive
4. 标准的数据连接

### 3. DataFrame

与RDD类似，DataFrame也是一个分布式数据容器。然而DataFrame更像传统数据库的二维表格，除了数据以外，还记录数据的结构信息，即schema。

同时，与Hive类似，DataFrame也支持嵌套数据类型（struct、array和map）。从API易用性的角度上看，DataFrame API提供的是一套高层的关系操作，比函数式的RDD API要更加友好，门槛更低。

![](https://img-blog.csdnimg.cn/20200309202148403.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NDg2NTU3NA==,size_16,color_FFFFFF,t_70)

上图直观地体现了DataFrame和RDD的区别。

左侧的RDD[Person]虽然以Person为类型参数，但Spark框架本身不了解Person类的内部结构。

右侧的DataFrame却提供了详细的结构信息，使得Spark SQL可以清楚地知道该数据集中包含哪些列，每列的名称和类型各是什么。

**DataFrame是为数据提供了Schema的视图**

可以把它当做数据库中的一张表来对待，DataFrame也是 **懒执行**的。

性能上比RDD要高，主要原因：优化的执行计划：查询计划通过Spark catalyst optimiser进行优化。

~~~scala
//解释:
//1.现在存在两个RDD
val rdd1 = sc.makeRDD(List((1,"zhangsan"),(2,"lisi"),(3."wangwu")))
val rdd2 = sc.makeRDD(List((1,"tom"),(2,"jack"),(3."Linda")))
//2.需求:将元组第一位=1的元组留下
// 那么我们会这样做:
val rdd3 = rdd1.join(rdd2)
rdd3.filter(xxx)
//众所周知,join算子是要进行shuffle的,如果先进行join，性能势必会慢
//3.但是呢，如果我们这样做:
val rdd4 = rdd1.filter(xxx)
val rdd5 = rdd2.filter(xxx)
//4.在来进行join
val rdd6 = rdd4.join(rdd5)
//Spark SQL的查询优化器正是这样做的  
~~~

简而言之，逻辑查询计划优化就是一个利用基于关系代数的等价变换，将高成本的操作替换为低成本操作的过程



### 4. DataSet

1. Dataframe API的一个扩展，是Spark最新的数据抽象；

2. 用户友好的API风格，既具有类型安全检查也具有Dataframe的查询优化特性；

3. Dataset支持编解码器，当需要访问非堆上的数据时可以避免反序列化整个对象，提高了效率；

4. 样例类被用来在Dataset中定义数据的结构信息，样例类中每个属性的名称直接映射到DataSet中的字段名称

5.  Dataframe是Dataset的特列，DataFrame=Dataset[Row] ，可以通过as方法将Dataframe转换为Dataset；

   ps：Row是一个类型，跟Car、Person这些的类型一样，所有的表结构信息我都用Row来表示；

6. DataSet是强类型的。比如可以有Dataset[Car]，Dataset[Person]；

7. DataFrame只是知道字段，但是不知道字段的类型，所以在执行这些操作的时候是没办法在编译的时候检查是否类型失败的，比如你可以对一个String进行减法操作，在执行的时候才报错，

   DataSet不仅仅知道字段，而且知道字段类型，所以有更严格的错误检查。就跟JSON对象和类对象之间的类比



## 二.SparkSQL使用

### 1. SparkSession

在老的版本中，SparkSQL提供两种SQL查询起始点：

1. SQLContext，用于Spark自己提供的SQL查询；
2. HiveContext，用于连接Hive的查询；

SparkSession是Spark最新的SQL查询起始点，实质上是SQLContext和HiveContext的组合，所以在SQLContext和HiveContext上可用的API在SparkSession上同样是可以使用的。

SparkSession内部封装了sparkContext，所以计算实际上是由sparkContext完成的。



### 2. DataFrame

#### 2.1 创建

在Spark SQL中SparkSession是创建DataFrame和执行SQL的入口，创建DataFrame有三种方式：

1. 通过Spark的数据源进行创建；

   ~~~scala
   package cn.xhjava.spark.sql

   import org.apache.spark.sql.{DataFrame, SparkSession}

   object SparkSQL_01_Demo2 {
     def main(args: Array[String]): Unit = {
       //初始化SparkSession
       val session: SparkSession = SparkSession.builder()
                                     .appName("SparkSQL_01_Demo2")
                                     .config("spark.some.config.option", "some-value")
                                     .master("local[3]")
                                     .getOrCreate()
   	//读取json文件创建
       val df: DataFrame = session.read.json("in/people.json")

       df.show()
       //释放资源
       session.stop()
     }
   }
   ~~~

   

2. 从一个存在的RDD进行转换；

   ~~~scala
   package cn.xhjava.spark.sql

   import org.apache.spark.rdd.RDD
   import org.apache.spark.sql.{DataFrame, SparkSession}

   object SparkSQL_01_Demo2 {
     def main(args: Array[String]): Unit = {
       //初始化SparkSession
       val spark: SparkSession = SparkSession.builder()
                                     .appName("SparkSQL_01_Demo2")
                                     .config("spark.some.config.option", "some-value")
                                     .master("local[3]")
                                     .getOrCreate()

       //1.创建RDD
       val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 21), (2, "lisi", 22), (3, "wangwu", 23)))
       //2.rdd--->DF
       //进行转换之前,需要隐式转换规则
       //这里的spark不是包名的含义,而是SparkSession对象名字
       import spark.implicits._
       val df: DataFrame = rdd.toDF("id","name","age")
       
       df.show()
       //释放资源
       spark.stop()
     }

   }

   ~~~

   

3. 从Hive Table进行查询返回；



#### 2.2 SQL语法

~~~scala
//1.创建一个DataFrame
scala> val df = spark.read.json("/opt/module/spark/examples/src/main/resources/people.json")
df: org.apache.spark.sql.DataFrame = [age: bigint, name: string]
//2.对DataFrame创建一个临时表
scala> df.createOrReplaceTempView("people")
//3.通过SQL语句实现查询全表
scala> val sqlDF = spark.sql("SELECT * FROM people")
sqlDF: org.apache.spark.sql.DataFrame = [age: bigint, name: string]
//4.结果展示
scala> sqlDF.show
+----+-------+
| age|   name|
+----+-------+
|null|Michael|
|  30|   Andy|
|  19| Justin|
+----+-------+
//注意：临时表是Session范围内的，Session退出后，表就失效了。如果想应用范围内有效，可以使用全局表。
//注意使用全局表时需要全路径访问，如：global_temp.people
//5.对于DataFrame创建一个全局表
scala> df.createGlobalTempView("people")
//6.通过SQL语句实现查询全表
scala> spark.sql("SELECT * FROM global_temp.people").show()
+----+-------+
| age|   name|
+----+-------+
|null|Michael|
|  30|   Andy|
|  19| Justin|

scala> spark.newSession().sql("SELECT * FROM global_temp.people").show()
+----+-------+
| age|   name|
+----+-------+
|null|Michael|
|  30|   Andy|
|  19| Justin|
+----+-------+
~~~

#### 2.3 DSL语法

~~~scala
//1.创建一个DateFrame
scala> spark.read.
csv   format   jdbc   json   load   option   options   orc   parquet   schema   table   text   textFile
//2.查看DataFrame的Schema信息
scala> df.printSchema
root
 |-- age: long (nullable = true)
 |-- name: string (nullable = true)
//3.只查看”name”列数据
scala> df.select("name").show()
+-------+
|   name|
+-------+
|Michael|
|   Andy|
| Justin|
+-------+
//4.查看”name”列数据以及”age+1”数据
scala> df.select($"name", $"age" + 1).show()
+-------+---------+
|   name|(age + 1)|
+-------+---------+
|Michael|     null|
|   Andy|       31|
| Justin|       20|
+-------+---------+
//5.查看”age”大于”21”的数据
scala> df.filter($"age" > 21).show()
+---+----+
|age|name|
+---+----+
| 30|Andy|
+---+----+
//6.按照”age”分组，查看数据条数
scala> df.groupBy("age").count().show()
+----+-----+
| age|count|
+----+-----+
|  19|     1|
|null|     1|
|  30|     1|
+----+-----+
~~~



#### 2.4 RDD-->DataFrame

注意：如果需要RDD与DF或者DS之间操作，那么都需要引入 

import spark.implicits._ 

~~~scala
//初始化SparkSession
val spark: SparkSession = SparkSession.builder()
                                  .appName("SparkSQL_01_Demo2")
                                  .config("spark.some.config.option", "some-value")
                                  .master("local[3]")
                                  .getOrCreate()
import spark.implicits._
~~~

##### 2.4.1 手动转换

~~~scala
package cn.xhjava.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL_01_Demo2 {
  def main(args: Array[String]): Unit = {
    //初始化SparkSession
    val spark: SparkSession = SparkSession.builder()
                                  .appName("SparkSQL_01_Demo2")
                                  .config("spark.some.config.option", "some-value")
                                  .master("local[3]")
                                  .getOrCreate()
    import spark.implicits._

    //1.创建RDD
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 21), (2, "lisi", 22), (3, "wangwu", 23)))
    //2.rdd--->DF
	//手动转换
    val df: DataFrame = rdd.toDF("id","name","age")

    df.show()
    //释放资源
    spark.stop()
  }
}
~~~

##### 2.4.2 通过反射转换

~~~scala
package cn.xhjava.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL_01_Demo2 {

  //样例类
  case class People(id: Int, name: String, age: Int)

  def main(args: Array[String]): Unit = {
    //初始化SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("SparkSQL_01_Demo2")
      .config("spark.some.config.option", "some-value")
      .master("local[3]")
      .getOrCreate()
    import spark.implicits._

    //1.创建RDD
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 21), (2, "lisi", 22), (3, "wangwu", 23)))
    //2.rdd--->DF
    val rddPeople: RDD[People] = rdd.map(x => {
      People(x._1, x._2, x._3)
    })

    val df: DataFrame = rddPeople.toDF()
    df.show()
    //释放资源
    spark.stop()
  }

}

~~~



#### 2.5 RDD-->DataSet

##### 2.5.1 创建

~~~scala
package cn.xhjava.spark.sql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkSQL_01_Demo2 {

  //样例类
  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {
    //初始化SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("SparkSQL_01_Demo2")
      .config("spark.some.config.option", "some-value")
      .master("local[3]")
      .getOrCreate()
    import spark.implicits._

    val ds: Dataset[Person] = Seq(Person("Andy", 32)).toDS()

    //释放资源
    spark.stop()
  }
}
~~~

##### 2.5.2 RDD-->DataSet

~~~scala
package cn.xhjava.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * RDD--->DS
  */
object SparkSQL_03_Transform2 {
  def main(args: Array[String]): Unit = {
    //初始化SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("SparkSQL_03_Transform2")
      .config("spark.some.config.option", "some-value")
      .master("local[3]")
      .getOrCreate()

    //进行转换之前,需要隐式转换规则
    //这里的spark不是包名的含义,而是SparkSession对象名字
    import spark.implicits._

    //创建RDD
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 21), (2, "lisi", 22), (3, "wangwu", 23)))

    //转换为RS
    val userRDD: RDD[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }
    val ds: Dataset[User] = userRDD.toDS()
  
    spark.stop()
  }

  //增加一个样例类
  case class User(id: Int, name: String, age: Int)

}
~~~

##### 2.5.3 DataSet

~~~scala
package cn.xhjava.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * >DS--->RDD
  */
object SparkSQL_03_Transform2 {
  def main(args: Array[String]): Unit = {
    //初始化SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("SparkSQL_03_Transform2")
      .config("spark.some.config.option", "some-value")
      .master("local[3]")
      .getOrCreate()

    //进行转换之前,需要隐式转换规则
    //这里的spark不是包名的含义,而是SparkSession对象名字
    import spark.implicits._

    //创建RDD
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 21), (2, "lisi", 22), (3, "wangwu", 23)))

    val ds: Dataset[User] = userRDD.toDS()
    //转换为RDD
    val rdd2: RDD[User] = ds.rdd
   
    //释放资源
    spark.stop()
  }

  //增加一个样例类
  case class User(id: Int, name: String, age: Int)

}
~~~

#### 2.6 RDD，DF，DS

![](https://img-blog.csdnimg.cn/20200309210754577.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NDg2NTU3NA==,size_16,color_FFFFFF,t_70)

##### 2.6.1 RDD-->DF-->DS

~~~scala
package cn.xhjava.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * RDD-->DF-->DS-->DF-->RDD-->打印
  */
object SparkSQL_03_Transform {
  def main(args: Array[String]): Unit = {
    //初始化SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("SparkSQL_03_Transform")
      .config("spark.some.config.option", "some-value")
      .master("local[3]")
      .getOrCreate()



    //创建RDD
    val userRDD: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 21), (2, "lisi", 22), (3, "wangwu", 23)))
    //转换为DF
    //进行转换之前,需要隐式转换规则
    //这里的spark不是包名的含义,而是SparkSession对象名字
    import spark.implicits._
    val df: DataFrame = userRDD.toDF("id", "name", "age")

    //转换为DS
    val ds: Dataset[User] = df.as[User]

    //转换为DF
    val df2: DataFrame = ds.toDF()

    //转换为RDD
    val rdd: RDD[Row] = df2.rdd

    //打印
    rdd.foreach(row => {
      //与JDBC ResultSet一样
      println(row.getString(1))
    })
    //释放资源
    spark.stop()
  }

  //增加一个样例类
  case class User(id: Int, name: String, age: Int)
}
~~~

##### 2.6.2 RDD-->DS-->RDD

~~~scala
package cn.xhjava.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * RDD--->DS--->RDD--->打印
  */
object SparkSQL_03_Transform2 {
  def main(args: Array[String]): Unit = {
    //初始化SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("SparkSQL_03_Transform2")
      .config("spark.some.config.option", "some-value")
      .master("local[3]")
      .getOrCreate()

    //进行转换之前,需要隐式转换规则
    //这里的spark不是包名的含义,而是SparkSession对象名字
    import spark.implicits._

    //创建RDD
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 21), (2, "lisi", 22), (3, "wangwu", 23)))

    //转换为RS
    val userRDD: RDD[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }
    val ds: Dataset[User] = userRDD.toDS()
    //转换为RDD

    val rdd2: RDD[User] = ds.rdd
    //打印
    rdd2.foreach(println)
    //释放资源
    spark.stop()
  }

  //增加一个样例类
  case class User(id: Int, name: String, age: Int)
}
~~~

##### 2.6.3 联系与区别

 在SparkSQL中Spark为我们提供了两个新的抽象，分别是DataFrame和DataSet。

他们和RDD有什么区别呢？首先从版本的产生上来看：

RDD (Spark1.0) —> Dataframe(Spark1.3) —>Dataset(Spark1.6)

如果同样的数据都给到这三个数据结构，他们分别计算之后，都会给出相同的结果。不同是的他们的执行效率和执行方式。

在后期的Spark版本中，DataSet会逐步取代RDD和DataFrame成为唯一的API接口。

###### 2.6.3.1 三者共性

1. RDD、DataFrame、Dataset全都是spark平台下的分布式弹性数据集，为处理超大型数据提供便利

2. 三者都有惰性机制，在进行创建、转换，如map方法时，不会立即执行，只有在遇到Action如foreach时，三者才会开始遍历运算

3. 三者都会根据spark的内存情况自动缓存运算，这样即使数据量很大，也不用担心会内存溢出。

4. 三者都有partition的概念；

5. 三者有许多共同的函数，如filter，排序等；

6. 在对DataFrame和Dataset进行操作许多操作都需要这个包进行支持

   import spark.implicits._

7. DataFrame和Dataset均可使用模式匹配获取各个字段的值和类型

###### 2.6.3.2 三种区别

1. RDD

   1. RDD一般和spark mlib同时使用；
   2. RDD不支持sparksql操作；

2. DataFrame

   1. 与RDD和Dataset不同，DataFrame每一行的类型固定为Row，每一列的值没法直接访问，只有通过解析才能获取各个字段的值，如：

      ~~~scala
      testDF.foreach{
        line =>
          val col1=line.getAs[String]("col1")
          val col2=line.getAs[String]("col2")
      }

      ~~~

   2. DataFrame与Dataset一般不与sparkmlib同时使用；

   3. DataFrame与Dataset均支持sparksql的操作，比如select，groupby之类，还能注册临时表/视窗，进行sql语句操作，如：

      ~~~scala
      dataDF.createOrReplaceTempView("tmp")
      spark.sql("select  ROW,DATE from tmp where DATE is not null order by DATE").show(100,false)
      ~~~

   4. DataFrame与Dataset支持一些特别方便的保存方式，比如保存成csv，可以带上表头，这样每一列的字段名一目了然；

      ~~~scala
      //保存
      val saveoptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> "hdfs://hadoop102:9000/test")
      datawDF.write.format("com.atguigu.spark.csv").mode(SaveMode.Overwrite).options(saveoptions).save()
      //读取
      val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> "hdfs://hadoop102:9000/test")
      val datarDF= spark.read.options(options).format("com.atguigu.spark.csv").load()
      ~~~

3. Dataset

   1. Dataset和DataFrame拥有完全相同的成员函数，区别只是每一行的数据类型不同；
   2. DataFrame也可以叫Dataset[Row],每一行的类型是Row，不解析，每一行究竟有哪些字段，各个字段又是什么类型都无从得知，只能用上面提到的getAS方法或者共性中的第七条提到的模式匹配拿出特定字段。而Dataset中，每一行是什么类型是不一定的，在自定义了case class之后可以很自由的获得每一行的信息



#### 2.7 SparkSQL 程序

##### 2.7.1 导入依赖

~~~xml
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-sql_2.11</artifactId>
    <version>2.1.1</version>
</dependency>
~~~

##### 2.7.2 编码

~~~scala
package cn.xhjava.spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL_01_Demo {
  def main(args: Array[String]): Unit = {
    //初始化SparkSession
    val spark: SparkSession = SparkSession.builder()
                                  .appName("SparkSQL_01_Demo")
                                  .config("spark.some.config.option", "some-value")
                                  .master("local[3]")
                                  .getOrCreate()

    val peopleDF: DataFrame = spark.read.json("in/people.json")

    peopleDF.show()
    //释放资源
    spark.stop()
  }
}
~~~

#### 2.8 用户自定义函数

##### 2.8.1 UDF

~~~scala
scala> val df = spark.read.json("examples/src/main/resources/people.json")
df: org.apache.spark.sql.DataFrame = [age: bigint, name: string]

scala> df.show()
+----+-------+
| age|   name|
+----+-------+
|null|Michael|
|  30|   Andy|
|  19| Justin|
+----+-------+

scala> spark.udf.register("addName", (x:String)=> "Name:"+x)
res5: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedFunction(<function1>,StringType,Some(List(StringType)))

scala> df.createOrReplaceTempView("people")

scala> spark.sql("Select addName(name), age from people").show()
+-----------------+----+
|UDF:addName(name)| age|
+-----------------+----+
|     Name:Michael|null|
|        Name:Andy|  30|
|      Name:Justin|  19|
+-----------------+----+
~~~

##### 2.8.2 UDAF（弱类型）

~~~scala
package cn.xhjava.spark.sql

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * 用户自定义聚合函数(弱类型)
  */
object SparkSQL_04_UDAF {
  def main(args: Array[String]): Unit = {
    //初始化SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("SparkSQL_04_UDAF")
      .config("spark.some.config.option", "some-value")
      .master("local[3]")
      .getOrCreate()

    //自定义聚合函数
    val udaf = new MyAgeAvgFunction
    spark.udf.register("avgAge", udaf)

    val peopleDF: DataFrame = spark.read.json("in/people.json")

    peopleDF.createOrReplaceTempView("user")
    spark.sql("select avgAge(age) from user").show()


    //释放资源
    spark.stop()
  }

  //增加一个样例类
  case class User(id: Int, name: String, age: Int)

}

//申明用户自定义聚合函数(弱类型)
//1.继承UserDefinedAggregateFunction类
//2.实现其方法

class MyAgeAvgFunction extends UserDefinedAggregateFunction {

  //函数输入的数据结构
  override def inputSchema: StructType = {
    new StructType().add("age", LongType)
  }

  //计算时的数据结构
  override def bufferSchema: StructType = {
    new StructType().add("sum", LongType).add("count", LongType)
  }

  //函数返回的数据类型
  override def dataType: DataType = DoubleType

  //函数是否稳定
  override def deterministic: Boolean = true

  //计算之前缓冲区初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0l
    buffer(1) = 0l
  }


  //根据查询结果更新缓冲区数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  //将多个节点的缓冲区合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //sum
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    //count
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  //计算
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0) / buffer.getLong(1).toDouble
  }
}
~~~

##### 2.8.3 UDAF（强类型）

~~~scala
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
~~~



## 三. SparkSQL数据源

#### 3.1 通用的加载方法

##### 3.1.1 手动指定选项

Spark SQL的DataFrame接口支持多种数据源的操作。一个DataFrame可以进行RDDs方式的操作，也可以被注册为临时表。把DataFrame注册为临时表之后，就可以对该DataFrame执行SQL查询。

~~~scala
package cn.xhjava.spark.sql.datasource

import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * Spark SQL的默认数据源为Parquet格式。
  * 数据源为Parquet文件时，Spark SQL可以方便的执行所有的操作。
  * 修改配置项spark.sql.sources.default，可修改默认数据源格式。
  */
object Spark_datasource_A_parquet {
  def main(args: Array[String]): Unit = {
    //初始化SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("Spark_datasource_A_parquet")
      .config("spark.some.config.option", "some-value")
      .master("local[3]")
      .getOrCreate()

    val df: DataFrame = spark.read.parquet("in/users.parquet")
    df.show(100)
    df.write.save("")
  }
}

~~~



~~~scala
package cn.xhjava.spark.sql.datasource

import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * 当数据源格式不是parquet格式文件时，需要手动指定数据源的格式。
  * 数据源格式需要指定全名（例如：org.apache.spark.sql.parquet），
  * 如果数据源格式为内置格式，
  * 则只需要指定简称定json, parquet, jdbc, orc, libsvm, csv, text来指定数据的格式
  */
object Spark_datasource_B_othertype {
  def main(args: Array[String]): Unit = {
    //初始化SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("Spark_datasource_B_othertype")
      .config("spark.some.config.option", "some-value")
      .master("local[3]")
      .getOrCreate()

    //下面以json文件为例
    val df: DataFrame = spark.read.format("json").load("in/people.json")
    df.show(100)

    //另外,保存的格式也可以手动指定
    df.write.format("parquet").save("")
  }
}

~~~



~~~scala
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

~~~



##### 3.1.2 文件保存选项

~~~scala
package cn.xhjava.spark.sql.filesave

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


/**
  * 可以采用SaveMode执行存储操作，SaveMode定义了对数据的处理模式。
  * 需要注意的是，这些保存模式不使用任何锁定，不是原子操作。
  * 此外，当使用Overwrite方式执行时，在输出新数据之前原数据就已经被删除。SaveMode详细介绍如下表：
  */
object Spark_datasource_FileSave {
  def main(args: Array[String]): Unit = {
    //初始化SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("Spark_datasource_FileSave")
      .config("spark.some.config.option", "some-value")
      .master("local[3]")
      .getOrCreate()

    //下面以json文件为例
    val df: DataFrame = spark.read.format("json").load("in/people.json")

    df.write.format("").mode(SaveMode.Append).save()
    SaveMode.ErrorIfExists //如果文件存在，则报错
    SaveMode.Append //追加
    SaveMode.Overwrite //覆盖写入
    SaveMode.Ignore //数据存在则忽略

  }
}

~~~

#### 3.2 json

~~~scala
package cn.xhjava.spark.sql.json

import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * Spark SQL 能够自动推测 JSON数据集的结构，并将它加载为一个Dataset[Row].
  * 可以通过SparkSession.read.json()去加载一个 一个JSON 文件
  * 注意：这个JSON文件不是一个传统的JSON文件，每一行都得是一个JSON串。
  */
object Spark_Json {
  def main(args: Array[String]): Unit = {
    //初始化SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("Spark_datasource_A_parquet")
      .config("spark.some.config.option", "some-value")
      .master("local[3]")
      .getOrCreate()

    val df: DataFrame = spark.read.json("in/people.json")
    df.show(100)
    df.write.save("")
  }
}

~~~

#### 3.3 parquet

Parquet是一种流行的列式存储格式，可以高效地存储具有嵌套字段的记录。Parquet格式经常在Hadoop生态圈中被使用，它也支持Spark SQL的全部数据类型。Spark SQL 提供了直接读取和存储 Parquet 格式文件的方法。 

~~~scala
package cn.xhjava.spark.sql.datasource

import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * Spark SQL的默认数据源为Parquet格式。
  * 数据源为Parquet文件时，Spark SQL可以方便的执行所有的操作。
  * 修改配置项spark.sql.sources.default，可修改默认数据源格式。
  */
object Spark_datasource_A_parquet {
  def main(args: Array[String]): Unit = {
    //初始化SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("Spark_datasource_A_parquet")
      .config("spark.some.config.option", "some-value")
      .master("local[3]")
      .getOrCreate()

    val df: DataFrame = spark.read.parquet("in/users.parquet")
    df.show(100)
    df.write.save("")
  }
}

~~~

#### 3.5 JDBC

~~~scala
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

~~~

#### 3.6 Hive

Apache Hive是Hadoop上的SQL引擎，Spark SQL编译时可以包含Hive支持，也可以不包含。包含Hive支持的Spark SQL可以支持Hive表访问、UDF(用户自定义函数)以及 Hive 查询语言(HiveQL/HQL)等。需要强调的一点是，如果要在Spark SQL中包含Hive的库，并不需要事先安装Hive。一般来说，最好还是在编译Spark SQL时引入Hive支持，这样就可以使用这些特性了。如果你下载的是二进制版本的 Spark，它应该已经在编译时添加了 Hive 支持。 
若要把Spark SQL连接到一个部署好的Hive上，你必须把hive-site.xml复制到 Spark的配置文件目录中($SPARK_HOME/conf)。即使没有部署好Hive，Spark SQL也可以运行。 需要注意的是，如果你没有部署好Hive，Spark SQL会在当前的工作目录中创建出自己的Hive 元数据仓库，叫作 metastore_db。此外，如果你尝试使用 HiveQL 中的 CREATE TABLE (并非 CREATE EXTERNAL TABLE)语句来创建表，这些表会被放在你默认的文件系统中的 /user/hive/warehouse 目录中(如果你的 classpath 中有配好的 hdfs-site.xml，默认的文件系统就是 HDFS，否则就是本地文件系统)。

##### 3.6.1 内嵌hive

如果要使用内嵌的Hive，什么都不用做，直接用就可以了。 

可以通过添加参数初次指定数据仓库地址：

--conf spark.sql.warehouse.dir=hdfs://hadoop102/spark-wearhouse

注意：如果你使用的是内部的Hive，在Spark2.0之后，spark.sql.warehouse.dir用于指定数据仓库的地址，如果你需要是用HDFS作为路径，那么需要将core-site.xml和hdfs-site.xml 加入到Spark conf目录，否则只会创建master节点上的warehouse目录，查询时会出现文件找不到的问题，这是需要使用HDFS，则需要将metastore删除，重启集群。

##### 3.6.2 外部hive 

如果想连接外部已经部署好的Hive，需要通过以下几个步骤。

1. 将Hive中的hive-site.xml拷贝或者软连接到Spark安装目录下的conf目录下。

2. 打开spark shell，注意带上访问Hive元数据库的JDBC客户端

   ` bin/spark-shell  --jars mysql-connector-java-5.1.27-bin.jar`

##### 3.6.3 代码使用Hive

~~~java
object Test2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Test Spark SQL Speed")
    conf.set("spark.sql.warehouse.dir", "hdfs://master:8020/user/hive/warehouse")
    conf.set("spark.sql.autoBroadcastJoinThreshold","1073741824")
    val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    val small: DataFrame = spark.sql("select * from xh.mdm_all_tmp").toDF()
    small.show()
  }
}
~~~

## 四. SparkSQL优化

### broadcast

~~~java
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

~~~

