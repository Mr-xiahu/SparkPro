### A_Spark基本介绍

#### 一.Spark历史

![](https://img-blog.csdnimg.cn/20200304202259131.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NDg2NTU3NA==,size_16,color_FFFFFF,t_70)

所以，Yarn问世了，Yarn由ResourceManager和NodeManager组成

1. ResourceManager(RM)的主要作用
   1. 处理客户端的请求(Spark-submit提交job)
   2. 监控NodeManager(监控节点状态)
   3. 启动或监控ApplicationMaster，每一个运行在yarn上的程序，都存在一个ApplicationMaster，只是该AM是随机在任意一个NodeManager上创建的
   4. 资源的分配与调度
2. NodeManager(NM)的主要作用
   1. 管理单个节点上的资源
   2. 处理来自RM的命令
   3. 处理来自AM的命令
3. ApplicationMaster的主要作用
   1. 负责数据的切分
   2. 为应用程序申请资源，并分配给内部的任务使用
   3. 任务的监控和容错
4. Container
   1. contaier是yarn中的资源抽象，它封装了某个节点上的多维度资源，例如：内存，CPU，磁盘，网络等。
   2. 每一个NodeManager节点都存在一个Container。
   3. 例如：Spark，Flink的计算最终都是在Container中进行的。

![](https://img-blog.csdnimg.cn/20200304202632327.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NDg2NTU3NA==,size_16,color_FFFFFF,t_70)



正是由于Yarn的存在，Spark诞生了

![](https://img-blog.csdnimg.cn/20200304204639151.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NDg2NTU3NA==,size_16,color_FFFFFF,t_70)

#### 二. Spark的特点

**Apache Spark™**是用于大规模数据处理的统一分析引擎。

spark是一个实现快速通用的集群计算平台。它是通用内存并行计算框架，用来构建大型的、低延迟的数据分析应用程序。它扩展了MapReduce计算模型。高效的支撑更多计算模式，包括交互式查询和流处理。

spark的一个主要特点是能够在内存中进行计算，及时依赖磁盘进行复杂的运算，Spark依然比MapReduce更加高效。

##### 2.1 Spark的四大特性

###### 2.1.1、高效性

运行速度提高100倍。

Apache Spark使用最先进的DAG调度程序，查询优化程序和物理执行引擎，实现批量和流式数据的高性能。

![img](https://images2018.cnblogs.com/blog/1228818/201804/1228818-20180419210923327-1692975321.png)

###### 2.1.2、易用性

Spark支持Java、Python和Scala的API，还支持超过80种高级算法，使用户可以快速构建不同的应用。而且Spark支持交互式的Python和Scala的shell，可以非常方便地在这些shell中使用Spark集群来验证解决问题的方法。

![img](https://images2018.cnblogs.com/blog/1228818/201804/1228818-20180419211144845-2020753937.png)

###### 2.1.3、通用性

Spark提供了统一的解决方案。Spark可以用于批处理、交互式查询（Spark SQL）、实时流处理（Spark Streaming）、机器学习（Spark MLlib）和图计算（GraphX）。这些不同类型的处理都可以在同一个应用中无缝使用。Spark统一的解决方案非常具有吸引力，毕竟任何公司都想用统一的平台去处理遇到的问题，减少开发和维护的人力成本和部署平台的物力成本。

![img](https://images2018.cnblogs.com/blog/1228818/201804/1228818-20180419211451611-367631364.png)

###### 2.1.4、兼容性

Spark可以非常方便地与其他的开源产品进行融合。比如，Spark可以使用Hadoop的YARN和Apache Mesos作为它的资源管理和调度器，器，并且可以处理所有Hadoop支持的数据，包括HDFS、HBase和Cassandra等。这对于已经部署Hadoop集群的用户特别重要，因为不需要做任何数据迁移就可以使用Spark的强大处理能力。Spark也可以不依赖于第三方的资源管理和调度器，它实现了Standalone作为其内置的资源管理和调度框架，这样进一步降低了Spark的使用门槛，使得所有人都可以非常容易地部署和使用Spark。此外，Spark还提供了在EC2上部署Standalone的Spark集群的工具。

![img](https://images2018.cnblogs.com/blog/1228818/201804/1228818-20180419211657058-248260284.png)

[Mesos](https://mesos.apache.org/)：Spark可以运行在Mesos里面（Mesos 类似于yarn的一个资源调度框架）

[standalone](http://spark.apache.org/docs/latest/spark-standalone.html)：Spark自己可以给自己分配资源（master，worker）

[YARN](https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html)：Spark可以运行在yarn上面

 [Kubernetes](https://kubernetes.io/)：Spark接收 [Kubernetes](https://kubernetes.io/)的资源调度

##### 2.2 Spark的构成

Spark组成(BDAS)：全称伯克利数据分析栈，通过大规模集成算法、机器、人之间展现大数据应用的一个平台。也是处理大数据、云计算、通信的技术解决方案。

它的主要组件有：

**SparkCore**：将分布式数据抽象为弹性分布式数据集（RDD），实现了应用任务调度、RPC、序列化和压缩，并为运行在其上的上层组件提供API。

**SparkSQL**：Spark Sql 是Spark来操作结构化数据的程序包，可以让我使用SQL语句的方式来查询数据，Spark支持 多种数据源，包含Hive表，parquest以及JSON等内容。

**SparkStreaming**： 是Spark提供的实时数据进行流式计算的组件。

**MLlib**：提供常用机器学习算法的实现库。

**GraphX**：提供一个分布式图计算框架，能高效进行图计算。

**BlinkDB**：用于在海量数据上进行交互式SQL的近似查询引擎。

**Tachyon**：以内存为中心高容错的的分布式文件系统。



#### 三.Spark运行模式

##### 3.1 spark地址

1. 官网地址

   <http://spark.apache.org/>

2. 文档查看地址

   <https://spark.apache.org/docs/2.1.1/>

3. 下载地址

   <https://spark.apache.org/downloads.html>

##### 3.2 Spark内重要角色

###### 3.2.1 Driver（驱动器）

~~~
Spark的驱动器是执行开发程序中的main方法的进程。它负责开发人员编写的用来创建SparkContext、创建RDD，以及进行RDD的转化操作和行动操作代码的执行。如果你是用spark shell，那么当你启动Spark shell的时候，系统后台自启了一个Spark驱动器程序，就是在Spark shell中预加载的一个叫作 sc的SparkContext对象。
如果驱动器程序终止，那么Spark应用也就结束了。主要负责：
1）把用户程序转为作业（JOB）
2）跟踪Executor的运行状况
3）为执行器节点调度任务
4）UI展示应用运行状况

~~~

###### 3.2.2 Executor（执行器）

~~~
Spark Executor是一个工作进程，负责在 Spark 作业中运行任务，任务间相互独立。Spark 应用启动时，Executor节点被同时启动，并且始终伴随着整个 Spark 应用的生命周期而存在。如果有Executor节点发生了故障或崩溃，Spark 应用也可以继续执行，会将出错节点上的任务调度到其他Executor节点上继续运行。主要负责：
1）负责运行组成 Spark 应用的任务，并将结果返回给驱动器进程；
2）通过自身的块管理器（Block Manager）为用户程序中要求缓存的RDD提供内存式存储。RDD是直接缓存在Executor进程内的，因此任务可以在运行时充分利用缓存数据加速运算。
~~~

##### 4. Local模式

1. local 模式就是运行在一台计算机上的模式，通常用于本机的测试。它可用通过一下方式设置master:

   1. local : 所以的计算都运行在一个线程中，没有任何并行计算，一般用于本机测试。
   2. local[K] : 指定使用K个线程来运行，如果local[3]就是运行3个worker线程。通常CPU有几个Core就指定几个线程，最大化利用CPU计算能力
   3. local[*] : 按照Cpu最多core来设置线程

   基本语法:

   ~~~shell
   bin/spark-submit \
   --class <main-class>
   --master <master-url> \
   --deploy-mode <deploy-mode> \
   --conf <key>=<value> \
   ... # other options
   <application-jar> \
   [application-arguments]

   ~~~

   参数说明:

   ~~~
   --master 指定Master的地址，默认为Local
   --class: 你的应用的启动类 (如 org.apache.spark.examples.SparkPi)
   --deploy-mode: 是否发布你的驱动到worker节点(cluster) 或者作为一个本地客户端 (client) (default: client)*
   --conf: 任意的Spark配置属性， 格式key=value. 如果值包含空格，可以加引号“key=value” 
   application-jar: 打包好的应用jar,包含依赖. 这个URL在集群中全局可见。 比如hdfs:// 共享存储系统， 如果是 file:// path， 那么所有的节点的path都包含同样的jar
   application-arguments: 传给main()方法的参数
   --executor-memory 1G 指定每个executor可用内存为1G
   --total-executor-cores 2 指定每个executor使用的cup核数为2个

   ~~~

   ​


##### 5. Standalone模式

构建一个由Master+Slave构成的Spark集群，Spark运行在集群中。

![](https://img-blog.csdnimg.cn/20200304210812943.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NDg2NTU3NA==,size_16,color_FFFFFF,t_70)

#####  6. Yarn模式

###### 6.1 概述

Spark客户端直接连接Yarn，不需要额外构建Spark集群。有yarn-client和yarn-cluster两种模式，主要区别在于：**Driver程序的运行节点**。

yarn-client：Driver程序运行在客户端，适用于交互、调试，希望立即看到app的输出

yarn-cluster：Driver程序运行在由RM（ResourceManager）启动的AP（APPMaster）适用于生产环境。



###### 6.2 yarn运行流程

![](https://img-blog.csdnimg.cn/20200304211154521.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NDg2NTU3NA==,size_16,color_FFFFFF,t_70)



#### 四. WordCount

1. 创建Maven项目并且导入下面依赖

   ~~~xml
   <dependencies>
   	<dependency>
   		<groupId>org.apache.spark</groupId>
   		<artifactId>spark-core_2.11</artifactId>
   		<version>2.4.3</version>
   	</dependency>
   </dependencies>
   <build>
   	<finalName>WordCount</finalName>
   	<plugins>
   		<plugin>
   			<groupId>net.alchim31.maven</groupId>
   			<artifactId>scala-maven-plugin</artifactId>
   			<version>3.2.2</version>
   			<executions>
   				<execution>
   					<goals>
   						<goal>compile</goal>
   						<goal>testCompile</goal>
   					</goals>
   				</execution>
   			</executions>
   		</plugin>
   	</plugins>
   </build>
   ~~~

2. 编写代码

   ~~~scala
   package cn.xhjava.spark.start

   import org.apache.spark.{SparkConf, SparkContext}

   object WordCount2 {
     def main(args: Array[String]): Unit = {

       //1.创建SparkConf并设置App名称
       val conf = new SparkConf().setAppName("WC")

       //2.创建SparkContext，该对象是提交Spark App的入口
       val sc = new SparkContext(conf)

       //3.使用sc创建RDD并执行相应的transformation和action
       sc.textFile(args(0)).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _, 1).sortBy(_._2, false).saveAsTextFile(args(1))

       //4.关闭连接
       sc.stop()
     }

   }
   ~~~

3. 打包插件

   ~~~xml
   <build>
   	<finalName>WordCount</finalName>
   	<!--scala编译工具-->
   	<plugins>
   		<plugin>
   			<groupId>net.alchim31.maven</groupId>
   			<artifactId>scala-maven-plugin</artifactId>
   			<version>3.2.2</version>
   			<executions>
   				<execution>
   					<goals>
   						<goal>compile</goal>
   						<goal>testCompile</goal>
   					</goals>
   				</execution>
   			</executions>
   		</plugin>
   		<!--打包工具-->
   		<plugin>
   			<groupId>org.apache.maven.plugins</groupId>
   			<artifactId>maven-assembly-plugin</artifactId>
   			<version>3.0.0</version>
   			<configuration>
   				<archive>
   					<manifest>
   						<mainClass>cn.xhjava.spark.start.WordCount2</mainClass>
   					</manifest>
   				</archive>
   				<descriptorRefs>
   					<descriptorRef>jar-with-dependencies</descriptorRef>
   				</descriptorRefs>
   			</configuration>
   			<executions>
   				<execution>
   					<id>make-assembly</id>
   					<phase>package</phase>
   					<goals>
   						<goal>single</goal>
   					</goals>
   				</execution>
   			</executions>
   		</plugin>
   	</plugins>
   </build>
   ~~~

4. 打包集群测试

   ~~~shell
   bin/spark-submit \
   --class WordCount \
   --master spark://hadoop102:7077 \
   WordCount.jar \
   /word.txt \
   /out
   ~~~

   ​

