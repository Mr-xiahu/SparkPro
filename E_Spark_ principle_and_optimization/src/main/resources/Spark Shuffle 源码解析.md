## Spark Shuffle 源码解析

在划分stage时，最后一个stage称为finalStage，

它本质上是一个ResultStage对象，前面的所有stage被称为ShuffleMapStage。
ShuffleMapStage的结束伴随着shuffle文件的写磁盘。
ResultStage基本上对应代码中的action算子，即将一个函数应用在RDD的各个partition的数据集上，意味着一个job的运行结束

![](https://img-blog.csdnimg.cn/2020070512405725.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NDg2NTU3NA==,size_16,color_FFFFFF,t_70)

~~~scala
//org.apache.spark.scheduler.DAGScheduler#submitMissingTasks
case stage: ShuffleMapStage =>
  stage.pendingPartitions.clear()
  partitionsToCompute.map { id =>
    val locs = taskIdToLocations(id)
    val part = partitions(id)
    stage.pendingPartitions += id
    new ShuffleMapTask(stage.id, stage.latestInfo.attemptNumber,
      taskBinary, part, locs, properties, serializedTaskMetrics, Option(jobId),
      Option(sc.applicationId), sc.applicationAttemptId, stage.rdd.isBarrier())
  }

case stage: ResultStage =>
  partitionsToCompute.map { id =>
    val p: Int = stage.partitions(id)
    val part = partitions(p)
    val locs = taskIdToLocations(id)
    new ResultTask(stage.id, stage.latestInfo.attemptNumber,
      taskBinary, part, locs, id, properties, serializedTaskMetrics,
      Option(jobId), Option(sc.applicationId), sc.applicationAttemptId,
      stage.rdd.isBarrier())
  }
~~~

首先看看ResultStage 是怎么读取数据的

### 1. ResultStage

~~~java
//org.apache.spark.scheduler.ResultTask#runTask
func(context, rdd.iterator(partition, context))
  
//org.apache.spark.rdd.RDD#iterator
computeOrReadCheckpoint(split, context)
  
//org.apache.spark.rdd.RDD#computeOrReadCheckpoint
if (isCheckpointedAndMaterialized) {
  firstParent[T].iterator(split, context)
} else {
  compute(split, context)
}

//org.apache.spark.rdd.RDD#compute
//每个RDD都存在compute，所以需要看一下ShuffledRDD 的compute

//org.apache.spark.rdd.ShuffledRDD#compute
override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {
  val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
  // 这边有getReader()方法，用于去读取shuffle文件
  //shuffleManager 后面再说
  SparkEnv.get.shuffleManager.getReader(dep.shuffleHandle, split.index, split.index + 1, context)
    .read()
    .asInstanceOf[Iterator[(K, C)]]
}
~~~



### ShuffleMapStage

既然存在Shuffle Reader ，肯定也是存在Shuffle Writer，来看看ShuffleMapStage

~~~java
//org.apache.spark.scheduler.ShuffleMapTask#runTask
var writer: ShuffleWriter[Any, Any] = null
    try {
      val manager = SparkEnv.get.shuffleManager
      writer = manager.getWriter[Any, Any](dep.shuffleHandle, partitionId, context)
      //  这边有getWriter()方法，用于去写shuffle文件
      //  现在存在一个问题。比如说:目前的程序只要两个阶段,ResultStage，ShuffleMapStage.
      //  ResultStage 可以有getReader()用于读取数据，ShuffleMapStage有getWriter()写数据
      //  但是如果程序不止两个阶段,有三个阶段呢?ResultStage，ShuffleMapStage,ShuffleMapStage
      //  ShuffleMapStage里面是只能写入步能读取吗？
      //  rdd.iterator(partition, context) 就是用来读取的，先读取，再写入
      writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
      writer.stop(success = true).get
    } catch {
      ...
      ...
    }
~~~



###  2. Shuffle中的任务个数

~~~
我们知道，Spark Shuffle分为map阶段和reduce阶段，或者称之为ShuffleRead阶段和ShuffleWrite阶段，那么对于一次Shuffle，map过程和reduce过程都会由若干个task来执行，

那么map task和reduce task的数量是如何确定的呢？

假设Spark任务从HDFS中读取数据，那么初始RDD分区个数由该文件的split个数决定，也就是一个split对应生成的RDD的一个partition，我们假设初始partition个数为N。

初始RDD经过一系列算子计算后（假设没有执行repartition和coalesce算子进行重分区，则分区个数不变，仍为N，如果经过重分区算子，那么分区个数变为M），我们假设分区个数不变，当执行到Shuffle操作时，map端的task个数和partition个数一致，即map task为N个。

reduce端的stage默认取spark.default.parallelism这个配置项的值作为分区数，如果没有配置，则以map端的最后一个RDD的分区数作为其分区数（也就是N），那么分区数就决定了reduce端的task的个数。
~~~

简单而言： Shuffle 内任务的个数 = 分区数 



### 3. reduce端数据的读取 

map 端 :   Shuffle 操作 写入文件到hdfs

reduce端:  Shuffle 操作读取hdfs文件

根据stage的划分我们知道，map端task和reduce端task不在相同的stage中，map task位于ShuffleMapStage，reduce task位于ResultStage，map task会先执行，那么后执行的reducetask如何知道从哪里去拉取maptask落盘后的数据呢？

reduce端的数据拉取过程如下：

1. map task 执行完毕后会将计算状态以及磁盘小文件位置等信息封装到MapStatus对象中，然后由本进程中的MapOutPutTrackerWorker对象将mapStatus对象发送给Driver进程的MapOutPutTrackerMaster对象；


2. 在reduce task开始执行之前会先让本进程中的MapOutputTrackerWorker向Driver进程中的MapoutPutTrakcerMaster发动请求，请求磁盘小文件位置信息；

3.    当所有的Map task执行完毕后，Driver进程中的MapOutPutTrackerMaster就掌握了所有的磁盘小文件的位置信息。此时MapOutPutTrackerMaster会告诉MapOutPutTrackerWorker磁盘小文件的位置信息；


4.    完成之前的操作之后，由BlockTransforService去Executor0所在的节点拉数据，默认会启动五个子线程。每次拉取的数据量不能超过48M（reduce task每次最多拉取48M数据，将拉来的数据存储到Executor内存的20%内存中）。



### 4. ShuffleManager 

很多算子都会引起 RDD 中的数据进行重分区，新的分区被创建，旧的分区被合并或者被打碎，**在重分区的过程中，如果数据发生了跨节点移动，就被称为 Shuffle**

在 Spark 中， Shuffle 负责将 Map 端（这里的 Map 端可以理解为宽依赖的左侧）的处理的中间结果传输到 Reduce 端供 Reduce 端聚合（这里的 Reduce 端可以理解为宽依赖的右侧），它是 MapReduce 类型计算框架中最重要的概念，同时也是很消耗性能的步骤。

**Shuffle 体现了从函数式编程接口到分布式计算框架的实现**。

与 MapReduce 的 Sort-based Shuffle 不同，Spark 对 Shuffle 的实现方式有两种：

1. Hash Shuffle 
2. Sort-based Shuffle

这其实是一个优化的过程。在较老的版本中，Spark Shuffle 的方式可以通过 spark.shuffle.manager 配置项进行配置，而在最新的 Spark 版本中，已经去掉了该配置，统一称为 Sort-based Shuffle。

#### 1. HashShuffle

在 Spark 1.6.3 之前， Hash Shuffle 都是 Spark Shuffle 的解决方案之一。 Shuffle 的过程一般分为两个部分：Shuffle Write 和 Shuffle Fetch，前者是 Map 任务划分分区、输出中间结果，而后者则是 Reduce 任务获取到的这些中间结果。Hash Shuffle 的过程如下图所示：

![](https://img-blog.csdnimg.cn/20200705164429139.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NDg2NTU3NA==,size_16,color_FFFFFF,t_70)

在图中，Shuffle Write 发生在一个节点上，该节点用来执行 Shuffle 任务的 CPU 核数为 2，

每个核可以同时执行两个任务，每个任务输出的分区数与 Reducer（这里的 Reducer 指的是 Reduce 端的 Executor）数相同，即为 3，

每个分区都有一个缓冲区（bucket）用来接收结果，每个缓冲区的大小由配置 spark.shuffle.file.buffer.kb 决定。这样每个缓冲区写满后，就会输出到一个文件段（filesegment），而 Reducer 就会去相应的节点拉取文件。

这样的实现很简单，但是问题也很明显。主要有两个：

1. 生成的中间结果文件数太大。理论上，每个 Shuffle 任务输出会产生 R 个文件（ R为Reducer 的个数），而 Shuffle 任务的个数往往由 Map 任务个数 M 决定，所以总共会生成 M * R 个中间结果文件，而往往在一个作业中 M 和 R 都是很大的数字，在大型作业中，经常会出现文件句柄数突破操作系统限制。
2. 缓冲区占用内存空间过大。单节点在执行 Shuffle 任务时缓存区大小消耗为 m * R * spark.shuffle.file.buffer.kb，m 为该节点运行的 Shuffle 任务数，如果一个核可以执行一个任务，m 就与 CPU 核数相等。这对于动辄有 32、64 物理核的服务器来说，是比不小的内存开销。



**HashShuffle 优化版本**

 Spark 推出过 File Consolidation 机制，旨在通过共用输出文件以降低文件数

![](https://img-blog.csdnimg.cn/20200705164712190.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NDg2NTU3NA==,size_16,color_FFFFFF,t_70)

每当 Shuffle 任务输出时，同一个 CPU 核心处理的 Map 任务的中间结果会输出到同分区的一个文件中，

然后 Reducer 只需一次性将整个文件拿到即可。这样，Shuffle 产生的文件数为 C（CPU 核数）* R。

 Spark 的 FileConsolidation 机制默认开启，可以通过 **spark.shuffle.consolidateFiles** 配置项进行配置。



#### 2. Sort-based Shuffle

在 Spark 先后引入了 Hash Shuffle 与 FileConsolidation 后，还是无法根本解决中间文件数太大的问题，所以 Spark 在 1.2 之后又推出了与 MapReduce 一样（你可以参照《Hadoop 海量数据处理》（第 2 版）的 Shuffle 相关章节）的 Shuffle 机制： **Sort-based Shuffle**，才真正解决了 Shuffle 的问题，再加上 Tungsten 计划的优化， Spark 的 Sort-based Shuffle 比 MapReduce 的 Sort-based Shuffle 青出于蓝

![](https://img-blog.csdnimg.cn/20200705165008514.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NDg2NTU3NA==,size_16,color_FFFFFF,t_70)

每个 Map 任务会最后只会输出两个文件（其中一个是索引文件），其中间过程采用的是与 MapReduce 一样的归并排序，但是会用索引文件记录每个分区的偏移量，输出完成后，Reducer 会根据索引文件得到属于自己的分区，在这种情况下，Shuffle 产生的中间结果文件数为 2 * M（M 为 Map 任务数）。



**补充**

在基于排序的 Shuffle 中， Spark 还提供了一种折中方案——Bypass Sort-based Shuffle，当 Reduce 任务小于 spark.shuffle.sort.bypassMergeThreshold 配置（默认 200）时，Spark Shuffle 开始按照 Hash Shuffle 的方式处理数据，而不用进行归并排序，只是在 Shuffle Write 步骤的最后，将其合并为 1 个文件，并生成索引文件。这样实际上还是会生成大量的中间文件，只是最后合并为 1 个文件并省去排序所带来的开销，该方案的准确说法是 Hash Shuffle 的Shuffle Fetch 优化版。

下面看一下源码：

~~~java
//org.apache.spark.scheduler.ShuffleMapTask#runTask
try {
  val manager = SparkEnv.get.shuffleManager
  // 看getWriter 方法
  writer = manager.getWriter[Any, Any](dep.shuffleHandle, partitionId, context)
  writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
  writer.stop(success = true).get
} catch {
  ...
  ...
}

//org.apache.spark.shuffle.ShuffleManager#getWriter
// ShuffleManager 是 trait ，需要看其实现类这里酒一个实现类SortShuffleManager

//org.apache.spark.shuffle.sort.SortShuffleManager#getWriter
handle match {
  // 这里是一个模式匹配，需要一个Handle，看一下哪里会返回handle呢
  case unsafeShuffleHandle: SerializedShuffleHandle[K @unchecked, V @unchecked] =>
    new UnsafeShuffleWriter(
      env.blockManager,
      shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver],
      context.taskMemoryManager(),
      unsafeShuffleHandle,
      mapId,
      context,
      env.conf)
  case bypassMergeSortHandle: BypassMergeSortShuffleHandle[K @unchecked, V @unchecked] =>
    new BypassMergeSortShuffleWriter(
      env.blockManager,
      shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver],
      bypassMergeSortHandle,
      mapId,
      context,
      env.conf)
  case other: BaseShuffleHandle[K @unchecked, V @unchecked, _] =>
    new SortShuffleWriter(shuffleBlockResolver, other, mapId, context)
}


//org.apache.spark.shuffle.sort.SortShuffleManager#registerShuffle
if (SortShuffleWriter.shouldBypassMergeSort(conf, dependency)) {
  // If there are fewer than spark.shuffle.sort.bypassMergeThreshold partitions and we don't
  // need map-side aggregation, then write numPartitions files directly and just concatenate
  // them at the end. This avoids doing serialization and deserialization twice to merge
  // together the spilled files, which would happen with the normal code path. The downside is
  // having multiple files open at a time and thus more memory allocated to buffers.
  //我们现在是要看怎么才去执行Bypass Sort-based Shuffle，所以需要看如何获取       BypassMergeSortShuffleHandle,所以看shouldBypassMergeSort方法
  new BypassMergeSortShuffleHandle[K, V](
    shuffleId, numMaps, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
} else if (SortShuffleManager.canUseSerializedShuffle(dependency)) {
  // Otherwise, try to buffer map outputs in a serialized form, since this is more efficient:
  new SerializedShuffleHandle[K, V](
    shuffleId, numMaps, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
} else {
  // Otherwise, buffer map outputs in a deserialized form:
  new BaseShuffleHandle(shuffleId, numMaps, dependency)
}


//org.apache.spark.shuffle.sort.SortShuffleWriter#shouldBypassMergeSort
def shouldBypassMergeSort(conf: SparkConf, dep: ShuffleDependency[_, _, _]): Boolean = {
  // We cannot bypass sorting if we need to do map-side aggregation.
  // 如果再map端使用了聚合，则不能使用cannot bypass sorting
  if (dep.mapSideCombine) {
    false
  } else {
    //获取配置文件中的spark.shuffle.sort.bypassMergeThreshold数据,默认为200
    val bypassMergeThreshold: Int = conf.getInt("spark.shuffle.sort.bypassMergeThreshold", 200)
    //如果 partition个数 <= bypassMergeThreshold,才会走new BypassMergeSortShuffleHandle
    dep.partitioner.numPartitions <= bypassMergeThreshold
  }
}

//走到这一步, new BypassMergeSortShuffleWriter()对象，便去执行对应write操作
~~~



Spark 在1.5 版本时开始了 Tungsten 计划，也在 1.5.0、 1.5.1、 1.5.2 的时候推出了一种 tungsten-sort 的选项，这是一种成果应用，类似于一种实验，该类型 Shuffle 本质上还是给予排序的 Shuffle，只是用 UnsafeShuffleWriter 进行 Map 任务输出，并采用了要在后面介绍的 BytesToBytesMap 相似的数据结构，把对数据的排序转化为对指针数组的排序，能够基于二进制数据进行操作，对 GC 有了很大提升。但是该方案对数据量有一些限制，随着 Tungsten 计划的逐渐成熟，该方案在 1.6 就消失不见了。