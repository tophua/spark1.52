/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rdd

import java.util.Random

import scala.collection.{ mutable, Map }
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.reflect.{ classTag, ClassTag }

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import org.apache.hadoop.io.{ BytesWritable, NullWritable, Text }
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapred.TextOutputFormat

import org.apache.spark._
import org.apache.spark.Partitioner._
import org.apache.spark.annotation.{ DeveloperApi, Experimental }
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.partial.BoundedDouble
import org.apache.spark.partial.CountEvaluator
import org.apache.spark.partial.GroupedCountEvaluator
import org.apache.spark.partial.PartialResult
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.{ BoundedPriorityQueue, Utils }
import org.apache.spark.util.collection.OpenHashMap
import org.apache.spark.util.random.{
  BernoulliSampler,
  PoissonSampler,
  BernoulliCellSampler,
  SamplingUtils
}

/**
 * A Resilient Distributed Dataset (RDD), the basic abstraction in Spark. Represents an immutable,
 * partitioned collection of elements that can be operated on in parallel. This class contains the
 * basic operations available on all RDDs, such as `map`, `filter`, and `persist`. In addition,
 * [[org.apache.spark.rdd.PairRDDFunctions]] contains operations available only on RDDs of key-value
 * pairs, such as `groupByKey` and `join`;
 * RDD是一个分布式弹性数据集,RDD是Spark的基本抽象,代表了一个不可变的、分区的、可以用于并行计算的数据集
 * 这个类包括了所有RDD共有的基本操作比如map, filter, persist。另外
 * org.apache.spark.rdd.PairRDDFunctions包括了只能用于key-value对类型的RDD的操作,比如groupByKey和join
 * [[org.apache.spark.rdd.DoubleRDDFunctions]] contains operations available only on RDDs of
 * Doubles; 包括了只能用于Double类型RDD的操作 and 
 * [[org.apache.spark.rdd.SequenceFileRDDFunctions]] contains operations available on RDDs that
 * can be saved as SequenceFiles.包括了能被保存为SequenceFile的RDD支持的操作。通过隐式转换,只要RDD的类型正确,
 * 相关的操作就自动可用
 * All operations are automatically available on any RDD of the right type (e.g. RDD[(Int, Int)]
 * through implicit.
 *
 * Internally, each RDD is characterized by five main properties:
 * 在内部,每个RDD都由五个主要属性来表征 * 
 *  - A list of partitions 分区列表
 *  - A function for computing each split 一个用于计算每个split的函数
 *  - A list of dependencies on other RDDs 对其它RDD的依赖
 *  - Optionally, a Partitioner(分区策略) for key-value RDDs (e.g. to say that the RDD is hash-partitioned)
 *  	可选:用于键值对类型的RDD使用的Partitioner
 *  - Optionally, a list of preferred locations(最佳位置) to compute each split on (e.g. block locations for
 *    an HDFS file)
 *    可选:计算每个split时优先使用的location(数据本地化 preferred locations)(比如一个HDFS文件的block的位置)
 *
 * All of the scheduling and execution in Spark is done based on these methods, allowing each RDD
 * to implement its own way of computing itself. Indeed, users can implement custom RDDs (e.g. for
 * reading data from a new storage system) by overriding these functions. Please refer to the
 * [[http://www.cs.berkeley.edu/~matei/papers/2012/nsdi_spark.pdf Spark paper]] for more details
 * on RDD internals.
 * RDD主构造器
  * 在泛型中，type T是被擦除的,ClassTag只包含实际运行时的类的类型,ClassTag会帮我们存储T的信息
 */
abstract class RDD[T: ClassTag](
    //在所有有父子关系的RDD,共享的是同一个SparkContext
    @transient private var _sc: SparkContext,
    //而子RDD的deps变量,也被赋值为一个List,里面包含一个OneToOneDependency实例,表明父RDD和子RDD之间的关系 
    @transient private var deps: Seq[Dependency[_]]) extends Serializable with Logging {
      if (classOf[RDD[_]].isAssignableFrom(elementClassTag.runtimeClass)) {
      // This is a warning instead of an exception in order to avoid breaking user programs that
      // might have defined nested RDDs without running jobs with them.
      logWarning("Spark does not support nested RDDs (see SPARK-5063)")
  }

  private def sc: SparkContext = {
    if (_sc == null) {
      throw new SparkException(
        "RDD transformations and actions can only be invoked by the driver, not inside of other " +
          "transformations; for example, rdd1.map(x => rdd2.values.count() * x) is invalid because " +
          "the values transformation and count action cannot be performed inside of the rdd1.map " +
          "transformation. For more information, see SPARK-5063.")
    }
    _sc
  }

  /** 
   *  Construct an RDD with just a one-to-one dependency on one parent
   *  RDD辅助构造器
   *  this是把父RDD的SparkContext(oneParent.context)和一个列表List(new OneToOneDependency(oneParent))),
   *  传入了另一个RDD的构造函数
   *   */
  def this(@transient oneParent: RDD[_]) =
    this(oneParent.context, List(new OneToOneDependency(oneParent)))

  private[spark] def conf = sc.conf
  // =======================================================================
  // Methods that should be implemented by subclasses of RDD
  // =======================================================================

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   * 计算给定的分区
    * RDD里compute()方法,负责接收来自上一个RDD或者数据源的input records
   */
  @DeveloperApi
  def compute(split: Partition, context: TaskContext): Iterator[T]

  /**
   * 返回RDD分区,通过子类在RDD返回分区的设置,此方法只调用一次，因此在其中实现耗时的计算是安全的.
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  protected def getPartitions: Array[Partition]

  /**
   * 只计算一次,计算RDD对父RDD的依赖
    * 由子类实现,以返回此RDD如何依赖父RDD,这种方法只会被调用一次,所以可以安全地执行耗时的计算。
   * Implemented by subclasses to return how this RDD depends on parent RDDs. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  protected def getDependencies: Seq[Dependency[_]] = deps

  /**
   * 指定优先位置,输入参数是split分片,输出结果是一组优先的节点位置
   * Optionally overridden by subclasses to specify placement preferences.
   */
  protected def getPreferredLocations(split: Partition): Seq[String] = Nil

  /** 
   *  Optionally overridden by subclasses to specify how they are partitioned. 
   *  key-value型的RDD是根据哈希来分区的,控制key分到哪个reduce
   *  */ 
  @transient val partitioner: Option[Partitioner] = None

  // =======================================================================
  // Methods and fields available on all RDDs 所有RDD上可用的方法和字段
  // =======================================================================

  /** The SparkContext that created this RDD.
    * 创建此RDD的SparkContext */
  def sparkContext: SparkContext = sc

  /** 
   *  A unique ID for this RDD (within its SparkContext).
    *  RDD的唯一ID（在SparkContext中）
   *  */
  val id: Int = sc.newRddId()

  /** 
   *  A friendly name for this RDD
   *  RDD名称
   *   */
  @transient var name: String = null
/**
 * this.type表示当前对象（this)的类型。this指代当前的对象。
 * this.type被用于变量,函数参数和函数返回值的类型声明
 */
  /** Assign a name to this RDD */
  def setName(_name: String): this.type = {
    name = _name
    this
  }

  /**
   * Mark this RDD for persisting using the specified level.
    * 标记此RDD以使用指定的级别进行持久化
   * this.type表示当前对象（this)的类型。this指代当前的对象。
   * this.type被用于变量,函数参数和函数返回值的类型声明
   * @param newLevel the target storage level
   * @param allowOverride whether to override any existing level with the new one
   */
  private def persist(newLevel: StorageLevel, allowOverride: Boolean): this.type = {
    // TODO: Handle changes of StorageLevel
    if (storageLevel != StorageLevel.NONE && newLevel != storageLevel && !allowOverride) {
      throw new UnsupportedOperationException(
        "Cannot change storage level of an RDD after it was already assigned a level")
    }
    // If this is the first time this RDD is marked for persisting, register it
    // with the SparkContext for cleanups and accounting. Do this only once.
    //如果这是RDD第一次标记为持久性,请注册与SparkContext进行清理和计费,只做一次
    if (storageLevel == StorageLevel.NONE) {
      sc.cleaner.foreach(_.registerRDDForCleanup(this))
      sc.persistRDD(this)
    }
    storageLevel = newLevel
    this
  }

  /**
   * 设置RDD存储级别在操作之后完成,这里只能分配RDD尚未确认的新存储级别,检查点是一个例别
   * Set this RDD's storage level to persist its values across operations after the first time
   * it is computed. This can only be used to assign a new storage level if the RDD does not
   * have a storage level set yet. Local checkpointing is an exception.
    * 设置此RDD的存储级别，以便在第一次操作之后保持其值计算,如果RDD没有,这只能用于分配新的存储级别
    *还有一个存储级别,本地检查点是一个例外。
   */
  def persist(newLevel: StorageLevel): this.type = {
    if (isLocallyCheckpointed) {
      //之前已经调用过localCheckpoint(),这里应该标记RDD待久化,在这里我们应该重写旧的存储级别,一个是由用户显式请求
      // This means the user previously called localCheckpoint(), which should have already
      // marked this RDD for persisting. Here we should override the old storage level with
      // one that is explicitly requested by the user (after adapting it to use disk).
      persist(LocalRDDCheckpointData.transformStorageLevel(newLevel), allowOverride = true)
    } else {
      persist(newLevel, allowOverride = false)
    }
  }

  /** 
   *  Persist this RDD with the default storage level (`MEMORY_ONLY`). 
   *  持久化RDD,默认存储级别MEMORY_ONLY,内存存储
   *  */
  def persist(): this.type = persist(StorageLevel.MEMORY_ONLY)

  /** 
   *  Persist this RDD with the default storage level (`MEMORY_ONLY`). 
   *  持久化RDD使用默认存储级别(内存存储)
   *  */
  def cache(): this.type = persist()

  /**
   * Mark the RDD as non-persistent, and remove all blocks for it from memory and disk.
   * 删除持久化RDD,同时删除内存和硬盘
   * @param blocking Whether to block until all blocks are deleted.
   * @return This RDD.
   */
  def unpersist(blocking: Boolean = true): this.type = {
    logInfo("Removing RDD " + id + " from persistence list")
    sc.unpersistRDD(id, blocking)
    storageLevel = StorageLevel.NONE
    this
  }

  /** 
   *  Get the RDD's current storage level, or StorageLevel.NONE if none is set.
   *  获得RDD当前的存储级别 
   *  */
  def getStorageLevel: StorageLevel = storageLevel

  // Our dependencies and partitions will be gotten by calling subclass's methods below, and will
  // be overwritten when we're checkpointed
  //我们的依赖和分区将通过调用子类的方法得到,我们的检查点并将被覆盖
  private var dependencies_ : Seq[Dependency[_]] = null
  @transient private var partitions_ : Array[Partition] = null

  /** 
   *  An Option holding our checkpoint RDD, if we are checkpointed
    *  如果我们检查点,我们的检查点RDD的选项
   *  */
  private def checkpointRDD: Option[CheckpointRDD[T]] = checkpointData.flatMap(_.checkpointRDD)

  /**
   * 返回当前RDD所依赖的RDD
   * Get the list of dependencies of this RDD, taking into account whether the
   * RDD is checkpointed or not.
   *Seq 有序重复数据
   */
  final def dependencies: Seq[Dependency[_]] = {
    checkpointRDD.map(r => List(new OneToOneDependency(r))).getOrElse {
      if (dependencies_ == null) {
        dependencies_ = getDependencies
      }
      dependencies_
    }
  }

  /**
   * Get the array of partitions of this RDD, taking into account whether the
   * RDD is checkpointed or not.
    * 获取这个RDD的分区数组,考虑到是否RDD是否被检查点
   */
  final def partitions: Array[Partition] = {
    checkpointRDD.map(_.partitions).getOrElse {
      if (partitions_ == null) {
        partitions_ = getPartitions
      }
      partitions_
    }
  }

  /**
   * Get the preferred locations of a partition(取得每个partition的优先位置), taking into account whether the
   * RDD is checkpointed.
   * 获取分区的首选位置,是否考虑到RDD是检查点
   */
  final def preferredLocations(split: Partition): Seq[String] = {
    checkpointRDD.map(_.getPreferredLocations(split)).getOrElse {
      getPreferredLocations(split)
    }
  }

  /**
   * Task的执行起点,计算由此开始
   * Internal method to this RDD; will read from cache if applicable(可用), or otherwise(否则) compute it.
   * This should ''not'' be called by users directly, but is available for implementors of custom
   * subclasses of RDD.
   * RDD内部方法,从缓存中读取可用,如果没有则计算它,
   */
  final def iterator(split: Partition, context: TaskContext): Iterator[T] = {
    if (storageLevel != StorageLevel.NONE) {
      //如果存储级别不是NONE 那么先检查是否有缓存,没有缓存则要进行计算
      SparkEnv.get.cacheManager.getOrCompute(this, split, context, storageLevel)
      //SparkEnv包含运行时节点所需要的环境信息
      //cacheManager负责调用BlockManager来管理RDD的缓存,如果当前RDD原来计算过并且把结果缓存起来.
      //接下来的运行都可以通过BlockManager来直接读取缓存后返回
    } else {
      //如果没有缓存,存在检查点时直接获取中间结果
      computeOrReadCheckpoint(split, context)
    }
  }

  /**
   * Return the ancestors(祖先) of the given RDD that are related to it only through a sequence of
   * narrow dependencies. This traverses the given RDD's dependency tree using DFS, but maintains
   * no ordering on the RDDs returned.
   * 方法获取RDD的所有直接或间接的NarrowDependency的RDD
   */
  private[spark] def getNarrowAncestors: Seq[RDD[_]] = {
    val ancestors = new mutable.HashSet[RDD[_]]

    def visit(rdd: RDD[_]) {
      //根据当前RDD获取依赖,过虑窄依赖
      val narrowDependencies = rdd.dependencies.filter(_.isInstanceOf[NarrowDependency[_]])
      //根据窄依赖关系获取RDD
      val narrowParents = narrowDependencies.map(_.rdd)
      //当前窄依赖关系不包含已添加ancestors到RDD
      val narrowParentsNotVisited = narrowParents.filterNot(ancestors.contains)
      narrowParentsNotVisited.foreach { parent =>
        ancestors.add(parent)
        visit(parent)
      }
    }

    visit(this)

    // In case there is a cycle, do not include the root itself
    //如果有一个循环,不要包括根本身
    ancestors.filterNot(_ == this).toSeq
  }

  /**
   * Compute an RDD partition or read it from a checkpoint if the RDD is checkpointing.
   * 首先检查当前RDD是否被isCheckpointed过,如果有,读取Checkpointed的数据,否则开始计算
   */
  private[spark] def computeOrReadCheckpoint(split: Partition, context: TaskContext): Iterator[T] =
    {    
      if (isCheckpointedAndMaterialized) {            
        firstParent[T].iterator(split, context)
      } else {
        compute(split, context)
      }
    }

  /**
   * Execute a block of code in a scope such that all new RDDs created in this body will
   * be part of the same scope. For more detail, see {{org.apache.spark.rdd.RDDOperationScope}}.
   * 新的RDDS将相同的范围的一部分执行的代码块。
   *
   * Note: Return statements are NOT allowed in the given body.
   * 在给定的名称替换函数不允许返回语句
   */
  private[spark] def withScope[U](body: => U): U = RDDOperationScope.withScope[U](sc)(body)

  // Transformations (return a new RDD)

  /**
   * Return a new RDD by applying a function to all elements of this RDD.
   * 返回一个新的分布式数据集,由每个原元素经过func函数转换后组成
   */
  def map[U: ClassTag](f: T => U): RDD[U] = withScope{
    //调用ClosureCleaner的clean清除闭包中的不能序列化的变更,防止RDD在网络传输过程中反序列化失败
    val cleanF = sc.clean(f)
    //这里this,会调用父类的构造函数RDD[U](prev),就是之前生成的HadoopRDD,MapPartitionsRDD的构造函数,, 
    //这个this(例如也就是hadoopRdd),会被赋值给prev
    new MapPartitionsRDD[U, T](this, (context, pid, iter) => iter.map(cleanF))
  }

  /**
   *  Return a new RDD by first applying a function to all elements of this
   *  RDD, and then flattening the results.
   *  类似于map,但是每一个输入元素,会被映射为0到多个输出元素
   *  （因此,func函数的返回值是一个Seq,而不是单一元素）
    *flatMap函数则是两个操作的集合——正是“先映射后扁平化”：
      1：同map函数一样：对每一条输入进行指定的操作,然后为每一条输入返回一个对象
      2：最后将所有对象合并为一个对象
   */
  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U] = withScope {
    val cleanF = sc.clean(f)
    new MapPartitionsRDD[U, T](this, (context, pid, iter) => iter.flatMap(cleanF))
  }

  /**
   * Return a new RDD containing only the elements that satisfy a predicate.
   * 对RDD中的数据单元进行过滤。如果过滤函数f返回true,则当前被验证的数据单元会被过滤。
   */
  def filter(f: T => Boolean): RDD[T] = withScope {
    val cleanF = sc.clean(f)
    new MapPartitionsRDD[T, T](
      this,
      (context, pid, iter) => iter.filter(cleanF),
      preservesPartitioning = true)
  }

  /**
   * Return a new RDD containing the distinct elements in this RDD.
   * 返回一个包含源数据集中所有不重复元素的新数据集
   */
  def distinct(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
    map(x => (x, null)).reduceByKey((x, y) => x, numPartitions).map(_._1)
  }

  /**
   * Return a new RDD containing the distinct elements in this RDD.
   * 返回一个包含源数据集中所有不重复元素的新数据集
   */
  def distinct(): RDD[T] = withScope {
    distinct(partitions.length)
  }

  /**
   * 该函数用于将RDD进行重分区,使用HashPartitioner。
   * 该函数其实就是coalesce函数第二个参数为true的实现
   * Return a new RDD that has exactly(正确) numPartitions partitions.
   * repartition用于增减rdd分区。coalesce特指减少分区,可以通过一次窄依赖的映射避免shuffle
   * Can increase or decrease the level of parallelism in this RDD. Internally, this uses
   * a shuffle to redistribute data.
    * 可以增加或减少此RDD中的并行度,在内部使用重新分发数据。
   *
   * If you are decreasing the number of partitions in this RDD, consider using `coalesce`,
   * which can avoid performing a shuffle.
    * 如果您减少此RDD中的分区数,请使用coalesce可以避免执行洗牌
   */
  def repartition(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
    coalesce(numPartitions, shuffle = true)
  }

  /**
   * Return a new RDD that is reduced into `numPartitions` partitions. 联合
   *该函数用于将RDD进行重分区,使用HashPartitioner。
          第一个参数为重分区的数目,第二个为是否进行shuffle,默认为false
   * This results in a narrow dependency, e.g. if you go from 1000 partitions
   * to 100 partitions, there will not be a shuffle, instead(替代) each of the 100
   * new partitions will claim 10 of the current partitions.
    * 这导致狭窄的依赖性，例如 如果你从1000个分区到100个分区，不会有洗牌，而是每个100个新分区将要求10个当前分区。
   *
   * However, if you're doing a drastic coalesce, e.g. to numPartitions = 1,
   * this may result in your computation taking place on fewer nodes than
   * you like (e.g. one node in the case of numPartitions = 1). To avoid this,
   * you can pass shuffle = true. This will add a shuffle step, but means the
   * current upstream partitions will be executed in parallel (per whatever
   * the current partitioning is).
    * 但是，如果你正在做一个激烈的聚结,例如 到numPartitions = 1,这可能导致您的计算发生在比您喜欢的更少的节点上
    * （例如，在numPartitions = 1的情况下，一个节点）。
    * 为了避免这种情况,你可以通过shuffle = true。 这将添加一个洗牌步骤，但是意味着当前的上游分区将被并行执行(无论当前的分区是什么)。
   *
   * Note: With shuffle = true, you can actually coalesce to a larger number
   * of partitions. This is useful if you have a small number of partitions,
   * say 100, potentially with a few partitions being abnormally large. Calling
   * coalesce(1000, shuffle = true) will result in 1000 partitions with the
   * data distributed using a hash partitioner.
    * 注意：随着shuffle = true,您可以实际合并到更多的分区,如果您有少量分区(例如100),可能会导致少量分区异常大,这很有用。
    * 调用coalesce(1000,shuffle = true)将导致1000个分区,数据使用哈希分割器分发。
   */
  def coalesce(numPartitions: Int, shuffle: Boolean = false)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
    if (shuffle) {
      /**
        * Distributes elements evenly across output partitions, starting from a random partition.
        * 从输出分区均匀分布元素，从随机分区开始
        * */
      val distributePartition = (index: Int, items: Iterator[T]) => {
        var position = (new Random(index)).nextInt(numPartitions)
        items.map { t =>
          // Note that the hash code of the key will just be the key itself. The HashPartitioner
          // will mod it with the number of total partitions.
          //请注意，密钥的哈希码本身就是密钥,哈希分配器将moded与总分区数。
          position = position + 1
          (position, t)
        }
      }: Iterator[(Int, T)]

      // include a shuffle step so that our upstream tasks are still distributed
      //包括一个洗牌的步骤,使我们的上游任务仍然分布
      new CoalescedRDD(
        new ShuffledRDD[Int, T, T](mapPartitionsWithIndex(distributePartition),
          new HashPartitioner(numPartitions)),
        numPartitions).values
    } else {
      new CoalescedRDD(this, numPartitions)
    }
  }

  /**
   * Return a sampled subset of this RDD.
   * 根据给定的随机种子seed,随机抽样出数量为frac的数据
   * @param withReplacement can elements be sampled multiple times (replaced when sampled out)
   * @param fraction expected size of the sample as a fraction of this RDD's size
   *  without replacement: probability that each element is chosen; fraction must be [0, 1]
   *  with replacement: expected number of times each element is chosen; fraction must be >= 0
   * @param seed seed for the random number generator
   */
  def sample(
    withReplacement: Boolean,
    fraction: Double,
    seed: Long = Utils.random.nextLong): RDD[T] = withScope {
    require(fraction >= 0.0, "Negative fraction value: " + fraction)
    if (withReplacement) {
      new PartitionwiseSampledRDD[T, T](this, new PoissonSampler[T](fraction), true, seed)
    } else {
      new PartitionwiseSampledRDD[T, T](this, new BernoulliSampler[T](fraction), true, seed)
    }
  }

  /**
   * Randomly(随机) splits this RDD with the provided weights.
   * 该函数根据weights权重,将一个RDD切分成多个RDD。
	 * 该权重参数为一个Double数组
   * 第二个参数为random的种子,基本可忽略
   * @param weights weights for splits, will be normalized if they don't sum to 1
   * @param seed random seed
   *
   * @return split RDDs in an array
   */
  def randomSplit(
    weights: Array[Double],
    seed: Long = Utils.random.nextLong): Array[RDD[T]] = withScope {
    val sum = weights.sum
    val normalizedCumWeights = weights.map(_ / sum).scanLeft(0.0d)(_ + _)
    normalizedCumWeights.sliding(2).map { x =>
      randomSampleWithRange(x(0), x(1), seed)
    }.toArray
  }

  /**
   * Internal method exposed for Random Splits in DataFrames. Samples an RDD given a probability
   * range.
    * 暴露在数据帧中的随机分割的内部方法,给出RDD给出概率范围。
   * @param lb lower bound to use for the Bernoulli sampler
   * @param ub upper bound to use for the Bernoulli sampler
   * @param seed the seed for the Random number generator
   * @return A random sub-sample of the RDD without replacement.
   */
  private[spark] def randomSampleWithRange(lb: Double, ub: Double, seed: Long): RDD[T] = {
    this.mapPartitionsWithIndex({ (index, partition) =>
      val sampler = new BernoulliCellSampler[T](lb, ub)
      sampler.setSeed(seed + index)
      sampler.sample(partition)
    }, preservesPartitioning = true)
  }

  /**
   * Return a fixed-size sampled subset of this RDD in an array
   *返回一个数组,在数据集中随机采样num个元素组成,可以选择是否用随机数替换不足的部分,
   * Seed用于指定的随机数生成器种子
   * @param withReplacement whether sampling is done with replacement
   * @param num size of the returned sample
   * @param seed seed for the random number generator
   * @return sample of specified size in an array
   */
  // TODO: rewrite this without return statements so we can wrap it in a scope
  def takeSample(
    withReplacement: Boolean,
    num: Int,
    seed: Long = Utils.random.nextLong): Array[T] = {
    val numStDev = 10.0

    if (num < 0) {
      throw new IllegalArgumentException("Negative number of elements requested")
    } else if (num == 0) {
      return new Array[T](0)
    }

    val initialCount = this.count()
    if (initialCount == 0) {
      return new Array[T](0)
    }

    val maxSampleSize = Int.MaxValue - (numStDev * math.sqrt(Int.MaxValue)).toInt
    if (num > maxSampleSize) {
      throw new IllegalArgumentException("Cannot support a sample size > Int.MaxValue - " +
        s"$numStDev * math.sqrt(Int.MaxValue)")
    }

    val rand = new Random(seed)
    if (!withReplacement && num >= initialCount) {
      return Utils.randomizeInPlace(this.collect(), rand)
    }

    val fraction = SamplingUtils.computeFractionForSampleSize(num, initialCount,
      withReplacement)

    var samples = this.sample(withReplacement, fraction, rand.nextInt()).collect()

    // If the first sample didn't turn out large enough, keep trying to take samples;
    // this shouldn't happen often because we use a big multiplier for the initial size
    //如果第一个样本没有足够大,不断尝试采样;这不应该经常发生,因为我们使用一个大的乘数为初始尺寸
    var numIters = 0
    while (samples.length < num) {
      logWarning(s"Needed to re-sample due to insufficient sample size. Repeat #$numIters")
      samples = this.sample(withReplacement, fraction, rand.nextInt()).collect()
      numIters += 1
    }

    Utils.randomizeInPlace(samples, rand).take(num)
  }

  /**
   * Return the union of this RDD and another one. Any identical elements will appear multiple
   * times (use `.distinct()` to eliminate them).
    * 返回这个RDD和另一个进行合并,不去重, 任何相同的元素将出现多次（使用.distinct（）来消除它们）。
   */
  def union(other: RDD[T]): RDD[T] = withScope {
    if (partitioner.isDefined && other.partitioner == partitioner) {
      new PartitionerAwareUnionRDD(sc, Array(this, other))
    } else {
      new UnionRDD(sc, Array(this, other))
    }
  }

  /**
   * Return the union of this RDD and another one. Any identical elements will appear multiple
   * times (use `.distinct()` to eliminate them).
    * 返回这个RDD和另一个进行合并,不去重, 任何相同的元素将出现多次（使用.distinct（）来消除它们）。
   */
  def ++(other: RDD[T]): RDD[T] = withScope {
    this.union(other)
  }

  /**
   * Return this RDD sorted by the given key function.
   * 这个方法将RDD的数据排序并存入新的RDD,第一个参数传入方法指定排序key,第二个参数指定是逆序还是顺序
   * sortBy根据给定的排序k函数将RDD中的元素进行排序
   * 该函数最多可以传三个参数：
　 * 第一个参数是一个函数,该函数的也有一个带T泛型的参数,返回类型和RDD中元素的类型是一致的；
　 * 第二个参数是ascending,决定排序后RDD中的元素是升序还是降序,默认是true,也就是升序；
　 * 第三个参数是numPartitions,该参数决定排序后的RDD的分区个数,默认排序后的分区个数和排序之前的个数相等,即为this.partitions.size
   * 例如:
   * scala> var rdd1 = sc.makeRDD(Seq(3,6,7,1,2,0),2)
   * scala> rdd1.sortBy(x => x).collect
   * res1: Array[Int] = Array(0, 1, 2, 3, 6, 7) //默认升序

   * scala> rdd1.sortBy(x => x,false).collect
   * res2: Array[Int] = Array(7, 6, 3, 2, 1, 0)  //降序

   * RDD[K,V]类型
   * scala>var rdd1 = sc.makeRDD(Array(("A",2),("A",1),("B",6),("B",3),("B",7))) 
   * scala> rdd1.sortBy(x => x).collect
   * res3: Array[(String, Int)] = Array((A,1), (A,2), (B,3), (B,6), (B,7))
 
   * 按照V进行降序排序
   * scala> rdd1.sortBy(x => x._2,false).collect
   * res4: Array[(String, Int)] = Array((B,7), (B,6), (B,3), (A,2), (A,1))
   */
  def sortBy[K](
    f: (T) => K,
    ascending: Boolean = true,
    numPartitions: Int = this.partitions.length)(implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T] = withScope {
    this.keyBy[K](f)
      .sortByKey(ascending, numPartitions)
      .values
  }

  /**
   * Return the intersection of this RDD and another one. The output will not contain any duplicate
   * elements, even if the input RDDs did.
   * 该函数返回两个RDD的交集,并且去重,输出不会包含任何重复的元素。
   * Note that this method performs a shuffle internally.
    * 请注意，此方法在内部执行shuffle
   */
  def intersection(other: RDD[T]): RDD[T] = withScope {
    this.map(v => (v, null)).cogroup(other.map(v => (v, null)))
      .filter { case (_, (leftGroup, rightGroup)) => leftGroup.nonEmpty && rightGroup.nonEmpty }
      .keys
  }

  /**
   * Return the intersection of this RDD and another one. The output will not contain any duplicate
   * elements, even if the input RDDs did.
   * 该函数返回两个RDD的交集,并且去重,输出不会包含任何重复的元素。
   * Note that this method performs a shuffle internally.
    * 请注意,此方法在内部执行shuffle
   *
   * @param partitioner Partitioner to use for the resulting RDD
   */
  def intersection(
    other: RDD[T],
    partitioner: Partitioner)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
    this.map(v => (v, null)).cogroup(other.map(v => (v, null)), partitioner)
      .filter { case (_, (leftGroup, rightGroup)) => leftGroup.nonEmpty && rightGroup.nonEmpty }
      .keys
  }

  /**
   * Return the intersection of this RDD and another one. The output will not contain any duplicate
   * elements, even if the input RDDs did.  Performs a hash partition across the cluster
   *该函数返回两个RDD的交集,并且去重,输出不会包含任何重复的元素。 在集群中执行散列分区
   * Note that this method performs a shuffle internally.
   * 请注意,此方法在内部执行shuffle
   * @param numPartitions How many partitions to use in the resulting RDD
   */
  def intersection(other: RDD[T], numPartitions: Int): RDD[T] = withScope {
    intersection(other, new HashPartitioner(numPartitions))
  }

  /**
   * Return an RDD created by coalescing(聚合) all elements within each partition into an array.
   * 将RDD中每一个分区中类型为T的元素转换成Array[T],这样每一个分区就只有一个数组元素
   * 例如:
   * scala> var rdd = sc.makeRDD(1 to 10,3)
	 * scala> rdd.partitions.size
	 * res33: Int = 3  //该RDD有3个分区
	 * scala> rdd.glom().collect
	 * res35: Array[Array[Int]] = Array(Array(1, 2, 3), Array(4, 5, 6), Array(7, 8, 9, 10))
	 * //glom将每个分区中的元素放到一个数组中,这样,结果就变成了3个数组
   */
  def glom(): RDD[Array[T]] = withScope {
    new MapPartitionsRDD[Array[T], T](this, (context, pid, iter) => Iterator(iter.toArray))
  }

  /**
   * 笛卡尔积
   * Return the Cartesian product of this RDD and another one, that is, the RDD of all pairs of
   * elements (a, b) where a is in `this` and b is in `other`.
    * 返回此RDD的笛卡尔乘积，另一个，即所有成对元素（a，b）的RDD，其中a为“this”，b为“other”。
   */
  def cartesian[U: ClassTag](other: RDD[U]): RDD[(T, U)] = withScope {
    new CartesianRDD(sc, this, other)
  }

  /**
   * 在一个由（K,V）对组成的数据集上调用,返回一个（K,Seq[V])对的数据集。
   * 注意：默认情况下,使用8个并行任务进行分组,你可以传入numTask可选参数,根据数据量设置不同数目的Task
    *返回分组项的RDD,每个组由一个键和一系列元素组成映射到该键,每组中的元素排序是不能保证的,每次评估结果RDD时,可能会有所不同。
   * Return an RDD of grouped items. Each group consists of a key and a sequence of elements
   * mapping to that key. The ordering of elements within each group is not guaranteed, and
   * may even differ each time the resulting RDD is evaluated.
   *
   * Note: This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using [[PairRDDFunctions.aggregateByKey]]
   * or [[PairRDDFunctions.reduceByKey]] will provide much better performance.
   */
  def groupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = withScope {
    groupBy[K](f, defaultPartitioner(this))
  }

  /**
   * Return an RDD of grouped elements. Each group consists of a key and a sequence of elements
   * mapping to that key. The ordering of elements within each group is not guaranteed, and
   * may even differ each time the resulting RDD is evaluated.
   *返回分组项目的RDD。每个组由一个键和一系列元素组成映射到该键,每个组中的元素的排序不能保证,并且每次得到的RDD被评估时甚至可能不同。
   * Note: This operation may be very expensive(耗时). If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key(如果你要分在每个键上执行聚合),
   * using [[PairRDDFunctions.aggregateByKey]] or [[PairRDDFunctions.reduceByKey]] 
   * will provide much better performance(将提供更好性能).
    *注意：此操作可能非常昂贵。如果您正在分组以执行使用[[PairRDDFunctions.aggregateByKey]]或[[PairRDDFunctions.reduceByKey]]
    * 在每个键上的聚合（如和或平均值）将提供更好的性能。
   */
  def groupBy[K](
    f: T => K,
    numPartitions: Int)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = withScope {
    groupBy(f, new HashPartitioner(numPartitions))
  }

  /**
   * Return an RDD of grouped items. Each group consists of a key and a sequence of elements
   * mapping to that key. The ordering of elements within each group is not guaranteed, and
   * may even differ each time the resulting RDD is evaluated.
    * 返回分组项目的RDD。每个组由一个键和一系列元素组成映射到该键,每个组中的元素的排序不能保证,并且每次得到的RDD被评估时甚至可能不同。
   *
   * Note: This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using [[PairRDDFunctions.aggregateByKey]]
   * or [[PairRDDFunctions.reduceByKey]] will provide much better performance.
    * 注意：此操作可能非常昂贵。 如果您正在分组以执行使用[[PairRDDFunctions.aggregateByKey]]或[[PairRDDFunctions.reduceByKey]]
    * 在每个键上的聚合（如和或平均值）将提供更好的性能。
   */
  def groupBy[K](f: T => K, p: Partitioner)(implicit kt: ClassTag[K], ord: Ordering[K] = null): RDD[(K, Iterable[T])] = withScope {
    val cleanF = sc.clean(f)
    this.map(t => (cleanF(t), t)).groupByKey(p)
  }

  /**
   * Return an RDD created by piping elements to a forked external process.
   * 把RDD数据通过ProcessBuilder创建额外的进程输出走
   */
  def pipe(command: String): RDD[String] = withScope {
    new PipedRDD(this, command)
  }

  /**
   * Return an RDD created by piping elements to a forked external process.
    * 将由管道元素创建的RDD返回到分叉的外部过程
   */
  def pipe(command: String, env: Map[String, String]): RDD[String] = withScope {
    new PipedRDD(this, command, env)
  }

  /**
   * Return an RDD created by piping elements to a forked external process.
   * The print behavior can be customized by providing two functions.
    * 将由管道元素创建的RDD返回到分叉的外部过程,可以通过提供两种功能来定制打印行为。
   *
   * @param command command to run in forked process. 在分叉过程中运行
   * @param env environment variables to set.
   * @param printPipeContext Before piping elements, this function is called as an opportunity
   *                         to pipe context data. Print line function (like out.println) will be
   *                         passed as printPipeContext's parameter.
   * @param printRDDElement Use this function to customize how to pipe elements. This function
   *                        will be called with each RDD element as the 1st parameter, and the
   *                        print line function (like out.println()) as the 2nd parameter.
   *                        An example of pipe the RDD data of groupBy() in a streaming way,
   *                        instead of constructing a huge String to concat all the elements:
   *                        def printRDDElement(record:(String, Seq[String]), f:String=&gt;Unit) =
   *                          for (e &lt;- record._2){f(e)}
   * @param separateWorkingDir Use separate working directories for each task.
   * @return the result RDD
   */
  def pipe(
    command: Seq[String],
    env: Map[String, String] = Map(),
    printPipeContext: (String => Unit) => Unit = null,
    printRDDElement: (T, String => Unit) => Unit = null,
    separateWorkingDir: Boolean = false): RDD[String] = withScope {
    new PipedRDD(this, command, env,
      if (printPipeContext ne null) sc.clean(printPipeContext) else null,
      if (printRDDElement ne null) sc.clean(printRDDElement) else null,
      separateWorkingDir)
  }

  /**
   * Return a new RDD by applying a function to each partition of this RDD.
   * 该函数和map函数类似,只不过映射函数的参数由RDD中的每一个元素变成了RDD中每一个分区的迭代器。
   * 如果在映射的过程中需要频繁创建额外的对象,使用mapPartitions要比map高效的过
   * 参数preservesPartitioning表示是否保留父RDD的partitioner分区信息
   * `preservesPartitioning` indicates whether the input function preserves the partitioner, which
   * should be `false` unless this is a pair RDD and the input function doesn't modify the keys.
   */
  def mapPartitions[U: ClassTag](
    f: Iterator[T] => Iterator[U],
    preservesPartitioning: Boolean = false): RDD[U] = withScope {
    val cleanedF = sc.clean(f)
     //这个this(例如也就是hadoopRdd),会被赋值给prev,然后调用RDD.scala
    new MapPartitionsRDD(
      this,
      (context: TaskContext, index: Int, iter: Iterator[T]) => cleanedF(iter),
      preservesPartitioning)
  }

  /**
   * Return a new RDD by applying a function to each partition of this RDD, while tracking the index
   * of the original partition.
   * 函数作用同mapPartitions,不过提供了两个参数,第一个参数为分区的索引
   * `preservesPartitioning` indicates whether the input function preserves the partitioner, which
   * should be `false` unless this is a pair RDD and the input function doesn't modify the keys.
   */
  def mapPartitionsWithIndex[U: ClassTag](
    f: (Int, Iterator[T]) => Iterator[U],
    preservesPartitioning: Boolean = false): RDD[U] = withScope {
    val cleanedF = sc.clean(f)
    new MapPartitionsRDD(
      this,
      (context: TaskContext, index: Int, iter: Iterator[T]) => cleanedF(index, iter),
      preservesPartitioning)
  }

  /**
   * :: DeveloperApi ::
    * 通过对这个RDD的每个分区应用一个函数来返回一个新的RDD,这是一个变种mapPartitions也将TaskContext传递给关闭。
   * Return a new RDD by applying a function to each partition of this RDD. This is a variant of
   * mapPartitions that also passes the TaskContext into the closure.
   *
   * `preservesPartitioning` indicates whether the input function preserves the partitioner, which
   * should be `false` unless this is a pair RDD and the input function doesn't modify the keys.
   */
  @DeveloperApi
  @deprecated("use TaskContext.get", "1.2.0")
  def mapPartitionsWithContext[U: ClassTag](
    f: (TaskContext, Iterator[T]) => Iterator[U],
    preservesPartitioning: Boolean = false): RDD[U] = withScope {
    val cleanF = sc.clean(f)
    val func = (context: TaskContext, index: Int, iter: Iterator[T]) => cleanF(context, iter)
    new MapPartitionsRDD(this, sc.clean(func), preservesPartitioning)
  }

  /**
   * Return a new RDD by applying a function to each partition of this RDD, while tracking the index
   * of the original partition.
    * 通过对该RDD的每个分区应用一个函数,同时跟踪原始分区的索引来返回一个新的RDD
   */
  @deprecated("use mapPartitionsWithIndex", "0.7.0")
  def mapPartitionsWithSplit[U: ClassTag](
    f: (Int, Iterator[T]) => Iterator[U],
    preservesPartitioning: Boolean = false): RDD[U] = withScope {
    mapPartitionsWithIndex(f, preservesPartitioning)
  }

  /**
   * Maps f over this RDD, where f takes an additional parameter of type A.  This
   * additional parameter is produced by constructA, which is called in each
   * partition with the index of that partition.
    * 在该RDD上映射f,其中f需要一个类型A的附加参数附加参数由constructA生成,在每个参数中调用分区与该分区的索引。
   */
  @deprecated("use mapPartitionsWithIndex", "1.0.0")
  def mapWith[A, U: ClassTag](constructA: Int => A, preservesPartitioning: Boolean = false)(f: (T, A) => U): RDD[U] = withScope {
    val cleanF = sc.clean(f)
    val cleanA = sc.clean(constructA)
    mapPartitionsWithIndex((index, iter) => {
      val a = cleanA(index)
      iter.map(t => cleanF(t, a))
    }, preservesPartitioning)
  }

  /**
   * FlatMaps f over this RDD, where f takes an additional parameter of type A.  This
   * additional parameter is produced by constructA, which is called in each
   * partition with the index of that partition.
   */
  @deprecated("use mapPartitionsWithIndex and flatMap", "1.0.0")
  def flatMapWith[A, U: ClassTag](constructA: Int => A, preservesPartitioning: Boolean = false)(f: (T, A) => Seq[U]): RDD[U] = withScope {
    val cleanF = sc.clean(f)
    val cleanA = sc.clean(constructA)
    mapPartitionsWithIndex((index, iter) => {
      val a = cleanA(index)
      iter.flatMap(t => cleanF(t, a))
    }, preservesPartitioning)
  }

  /**
   * Applies f to each element of this RDD, where f takes an additional parameter of type A.
   * This additional parameter is produced by constructA, which is called in each
   * partition with the index of that partition.
   */
  @deprecated("use mapPartitionsWithIndex and foreach", "1.0.0")
  def foreachWith[A](constructA: Int => A)(f: (T, A) => Unit): Unit = withScope {
    val cleanF = sc.clean(f)
    val cleanA = sc.clean(constructA)
    mapPartitionsWithIndex { (index, iter) =>
      val a = cleanA(index)
      iter.map(t => { cleanF(t, a); t })
    }
  }

  /**
   * Filters this RDD with p, where p takes an additional parameter of type A.  This
   * additional parameter is produced by constructA, which is called in each
   * partition with the index of that partition.
   */
  @deprecated("use mapPartitionsWithIndex and filter", "1.0.0")
  def filterWith[A](constructA: Int => A)(p: (T, A) => Boolean): RDD[T] = withScope {
    val cleanP = sc.clean(p)
    val cleanA = sc.clean(constructA)
    mapPartitionsWithIndex((index, iter) => {
      val a = cleanA(index)
      iter.filter(t => cleanP(t, a))
    }, preservesPartitioning = true)
  }

  /**
   * zip函数用于将两个RDD组合成Key/Value形式的RDD,这里默认两个RDD的partition数量以及元素数量都相同,否则会抛出异常
   * Zips this RDD with another one, returning key-value pairs with the first element in each RDD,
   * second element in each RDD, etc. Assumes that the two RDDs have the *same number of
   * partitions* and the *same number of elements in each partition* (e.g. one was made through
   * a map on the other).
   */
  def zip[U: ClassTag](other: RDD[U]): RDD[(T, U)] = withScope {
    zipPartitions(other, preservesPartitioning = false) { (thisIter, otherIter) =>
      new Iterator[(T, U)] {
        def hasNext: Boolean = (thisIter.hasNext, otherIter.hasNext) match {
          case (true, true)   => true
          case (false, false) => false
          case _ => throw new SparkException("Can only zip RDDs with " +
            "same number of elements in each partition")
        }
        def next(): (T, U) = (thisIter.next(), otherIter.next())
      }
    }
  }

  /**
   * Zip this RDD's partitions with one (or more) RDD(s) and return a new RDD by
   * applying a function to the zipped partitions. Assumes that all the RDDs have the
   * *same number of partitions*, but does *not* require them to have the same number
   * of elements in each partition.
   * zipPartitions函数将多个RDD按照partition组合成为新的RDD,该函数需要组合的RDD具有相同的分区数,但对于每个分区内的元素数量没有要求
   */
  def zipPartitions[B: ClassTag, V: ClassTag](rdd2: RDD[B], preservesPartitioning: Boolean)(f: (Iterator[T], Iterator[B]) => Iterator[V]): RDD[V] = withScope {
    new ZippedPartitionsRDD2(sc, sc.clean(f), this, rdd2, preservesPartitioning)
  }

  def zipPartitions[B: ClassTag, V: ClassTag](rdd2: RDD[B])(f: (Iterator[T], Iterator[B]) => Iterator[V]): RDD[V] = withScope {
    zipPartitions(rdd2, preservesPartitioning = false)(f)
  }

  def zipPartitions[B: ClassTag, C: ClassTag, V: ClassTag](rdd2: RDD[B], rdd3: RDD[C], preservesPartitioning: Boolean)(f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V]): RDD[V] = withScope {
    new ZippedPartitionsRDD3(sc, sc.clean(f), this, rdd2, rdd3, preservesPartitioning)
  }

  def zipPartitions[B: ClassTag, C: ClassTag, V: ClassTag](rdd2: RDD[B], rdd3: RDD[C])(f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V]): RDD[V] = withScope {
    zipPartitions(rdd2, rdd3, preservesPartitioning = false)(f)
  }

  def zipPartitions[B: ClassTag, C: ClassTag, D: ClassTag, V: ClassTag](rdd2: RDD[B], rdd3: RDD[C], rdd4: RDD[D], preservesPartitioning: Boolean)(f: (Iterator[T], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V]): RDD[V] = withScope {
    new ZippedPartitionsRDD4(sc, sc.clean(f), this, rdd2, rdd3, rdd4, preservesPartitioning)
  }

  def zipPartitions[B: ClassTag, C: ClassTag, D: ClassTag, V: ClassTag](rdd2: RDD[B], rdd3: RDD[C], rdd4: RDD[D])(f: (Iterator[T], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V]): RDD[V] = withScope {
    zipPartitions(rdd2, rdd3, rdd4, preservesPartitioning = false)(f)
  }

  // Actions (launch a job to return a value to the user program)

  /**
   * Applies a function f to all elements of this RDD.
   * foreach用于遍历RDD,将函数f应用于每一个元素。
   * 但要注意,如果对RDD执行foreach,只会在Executor端有效,而并不是Driver端
   */
  def foreach(f: T => Unit): Unit = withScope {
    val cleanF = sc.clean(f)
    sc.runJob(this, (iter: Iterator[T]) => iter.foreach(cleanF))
  }

  /**
   * Applies a function f to each partition of this RDD.
   * 在数据集的每一个分区上,运行函数func。这通常用于更新一个累加器变量,或者和外部存储系统做交互
   */
  def foreachPartition(f: Iterator[T] => Unit): Unit = withScope {
    val cleanF = sc.clean(f)
    sc.runJob(this, (iter: Iterator[T]) => cleanF(iter))
  }

  /**
   * Return an array that contains all of the elements in this RDD.
   * 在Driver的程序中,collect用于将一个RDD转换成数组
   */
  def collect(): Array[T] = withScope {
    //需要注意的是这里传入了一个函数,这个函数就是这个Job的要执行的任务,
    //它将会被包装并序列化后发送到要执行它的executor上,并在要处理的RDD上的每个分区上被调用执行
    val results = sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
    //表示可以传不定个数的参数
    //:_*让你把列表、数组、系列转为一个变长,传给接受不定长参数的函数
    //: _*是表示传入该函数(Array.concat)的参数的类型为任意类型且多参数，换言之，传入的参数会被转换为多个Array
    Array.concat(results: _*)
  }

  /**
   * Return an iterator that contains all of the elements in this RDD.
   *把所有数据以迭代器返回,rdd实现是调用sc.runJob(),每个分区迭代器转array,
   *收集到driver端再flatMap一次打散成大迭代器。理解为一种比较特殊的driver端cache
   * The iterator will consume as much memory as the largest partition in this RDD.
   *
   * Note: this results in multiple Spark jobs, and if the input RDD is the result
   * of a wide transformation (e.g. join with different partitioners), to avoid
   * recomputing the input RDD should be cached first.
    * 注意：这会导致多个Spark作业，如果输入RDD是结果进行广泛的转换(例如，与不同的分区器连接),以避免
    *重新计算输入RDD应该先被缓存。
   */
  def toLocalIterator: Iterator[T] = withScope {
    def collectPartition(p: Int): Array[T] = {
      sc.runJob(this, (iter: Iterator[T]) => iter.toArray, Seq(p)).head
    }
    (0 until partitions.length).iterator.flatMap(i => collectPartition(i))
  }

  /**
   * Return an array that contains all of the elements in this RDD.
    * 返回一个包含此RDD中所有元素的数组
   */
  @deprecated("use collect", "1.0.0")
  def toArray(): Array[T] = withScope {
    collect()
  }

  /**
   * Return an RDD that contains all matching values by applying `f`.
   * 在数据集的每一个元素上,运行函数func,以数组的形式,返回数据集的所有元素
   */
  def collect[U: ClassTag](f: PartialFunction[T, U]): RDD[U] = withScope {
    val cleanF = sc.clean(f)
    filter(cleanF.isDefinedAt).map(cleanF)
  }

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   * 返回在RDD中出现,并且不在other RDD中出现的元素(交集,相当于进行集合的差操作),不去重
   * Uses `this` partitioner/partition size, because even if `other` is huge, the resulting
   * RDD will be &lt;= us.
   * 例如:
   * scala> var rdd1 = sc.makeRDD(Seq(1,2,2,3))
   * res48: Array[Int] = Array(1, 2, 2, 3)
   * scala> var rdd2 = sc.makeRDD(3 to 4)
   * res49: Array[Int] = Array(3, 4)
   * scala> rdd1.subtract(rdd2).collect
   * res50: Array[Int] = Array(1, 2, 2)
   *
   */
  def subtract(other: RDD[T]): RDD[T] = withScope {
    subtract(other, partitioner.getOrElse(new HashPartitioner(partitions.length)))
  }

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   */
  def subtract(other: RDD[T], numPartitions: Int): RDD[T] = withScope {
    subtract(other, new HashPartitioner(numPartitions))
  }

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   * 但返回在RDD中出现,并且不在otherRDD中出现的元素,不去重
   */
  def subtract(
    other: RDD[T],
    p: Partitioner)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
    if (partitioner == Some(p)) {
      // Our partitioner knows how to handle T (which, since we have a partitioner, is
      // really (K, V)) so make a new Partitioner that will de-tuple our fake tuples
      val p2 = new Partitioner() {
        override def numPartitions: Int = p.numPartitions
        override def getPartition(k: Any): Int = p.getPartition(k.asInstanceOf[(Any, _)]._1)
      }
      // Unfortunately, since we're making a new p2, we'll get ShuffleDependencies
      // anyway, and when calling .keys, will not have a partitioner set, even though
      // the SubtractedRDD will, thanks to p2's de-tupled partitioning, already be
      // partitioned by the right/real keys (e.g. p).
      this.map(x => (x, null)).subtractByKey(other.map((_, null)), p2).keys
    } else {
      this.map(x => (x, null)).subtractByKey(other.map((_, null)), p).keys
    }
  }

  /**
   * 根据映射函数f(Func函数接受2个参数,返回一个值),对RDD中的元素进行二元计算,返回计算结果
   * Reduces the elements of this RDD using the specified commutative and
   * associative binary operator.
   * 例如:
   * scala> var rdd1 = sc.makeRDD(1 to 10,2)
   * scala> rdd1.reduce(_ + _)
   * res18: Int = 55
   */
  def reduce(f: (T, T) => T): T = withScope {
    val cleanF = sc.clean(f)
    val reducePartition: Iterator[T] => Option[T] = iter => {//不明白函数定义?
      if (iter.hasNext) {
        Some(iter.reduceLeft(cleanF))
      } else {
        None
      }
    }
    var jobResult: Option[T] = None
    //匿名函数
    val mergeResult = (index: Int, taskResult: Option[T]) => {
      if (taskResult.isDefined) {
        jobResult = jobResult match {
          case Some(value) => Some(f(value, taskResult.get))
          case None        => taskResult
        }
      }
    }
    sc.runJob(this, reducePartition, mergeResult)
    // Get the final result out of our Option, or throw an exception if the RDD was empty
    //从我们的选项中获取最终结果,如果RDD为空，则抛出异常
    jobResult.getOrElse(throw new UnsupportedOperationException("empty collection"))
  }

  /**
   * Reduces the elements of this RDD in a multi-level tree pattern.
   * 以多级树模式减少此RDD的元素。
   * @param depth suggested depth of the tree (default: 2)
   * @see [[org.apache.spark.rdd.RDD#reduce]]
   */
  def treeReduce(f: (T, T) => T, depth: Int = 2): T = withScope {
    require(depth >= 1, s"Depth must be greater than or equal to 1 but got $depth.")
    val cleanF = context.clean(f)
    //
    val reducePartition: Iterator[T] => Option[T] = iter => {
      if (iter.hasNext) {
        Some(iter.reduceLeft(cleanF))
      } else {
        None
      }
    }
    val partiallyReduced = mapPartitions(it => Iterator(reducePartition(it)))
    val op: (Option[T], Option[T]) => Option[T] = (c, x) => {
      if (c.isDefined && x.isDefined) {
        Some(cleanF(c.get, x.get))
      } else if (c.isDefined) {
        c
      } else if (x.isDefined) {
        x
      } else {
        None
      }
    }
    partiallyReduced.treeAggregate(Option.empty[T])(op, op, depth)
      .getOrElse(throw new UnsupportedOperationException("empty collection"))
  }

  /**
   * 聚合每个数据分片的数据。每个数据分片聚合之前可以设置一个初始值zeroValue
   * Aggregate the elements of each partition, and then the results for all the partitions, using a
   * given associative and commutative function and a neutral "zero value". The function
   * op(t1, t2) is allowed to modify t1 and return it as its result value to avoid object
   * allocation; however, it should not modify t2.
   *
   * This behaves somewhat differently from fold operations implemented for non-distributed
   * collections in functional languages like Scala. This fold operation may be applied to
   * partitions individually, and then fold those results into the final result, rather than
   * apply the fold to each element sequentially in some defined ordering. For functions
   * that are not commutative, the result may differ from that of a fold applied to a
   * non-distributed collection.
   */
  def fold(zeroValue: T)(op: (T, T) => T): T = withScope {
    // Clone the zero value since we will also be serializing it as part of tasks
    var jobResult = Utils.clone(zeroValue, sc.env.closureSerializer.newInstance())
    val cleanOp = sc.clean(op)
    val foldPartition = (iter: Iterator[T]) => iter.fold(zeroValue)(cleanOp)
    val mergeResult = (index: Int, taskResult: T) => jobResult = op(jobResult, taskResult)
    sc.runJob(this, foldPartition, mergeResult)
    jobResult
  }

  /**
   * Aggregate the elements of each partition, and then the results for all the partitions, using
   * given combine functions and a neutral "zero value". This function can return a different result
   * type, U, than the type of this RDD, T. Thus, we need one operation for merging a T into an U
   * and one operation for merging two U's, as in scala.TraversableOnce. Both of these functions are
   * allowed to modify and return their first argument instead of creating a new U to avoid memory
   * allocation.
   * 带初始值、reduce聚合、merge聚合三个完整条件的聚合方法。
   * rdd的做法是把函数传入分区里去做计算,最后汇总各分区的结果再一次combOp计算
   * 
   */
  def aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U = withScope {
    // Clone the zero value since we will also be serializing it as part of tasks
    var jobResult = Utils.clone(zeroValue, sc.env.serializer.newInstance())
    val cleanSeqOp = sc.clean(seqOp)
    val cleanCombOp = sc.clean(combOp)
    val aggregatePartition = (it: Iterator[T]) => it.aggregate(zeroValue)(cleanSeqOp, cleanCombOp)
    val mergeResult = (index: Int, taskResult: U) => jobResult = combOp(jobResult, taskResult)
    sc.runJob(this, aggregatePartition, mergeResult)
    jobResult
  }

  /**
   * Aggregates the elements of this RDD in a multi-level tree pattern.
   * 以多级树模式聚合此RDD的元素
   * @param depth suggested depth of the tree (default: 2)
   * @see [[org.apache.spark.rdd.RDD#aggregate]]
   */
  def treeAggregate[U: ClassTag](zeroValue: U)(
    seqOp: (U, T) => U,
    combOp: (U, U) => U,
    depth: Int = 2): U = withScope {
    require(depth >= 1, s"Depth must be greater than or equal to 1 but got $depth.")
    if (partitions.length == 0) {
      Utils.clone(zeroValue, context.env.closureSerializer.newInstance())
    } else {
      val cleanSeqOp = context.clean(seqOp)
      val cleanCombOp = context.clean(combOp)
      val aggregatePartition =
        (it: Iterator[T]) => it.aggregate(zeroValue)(cleanSeqOp, cleanCombOp)
      var partiallyAggregated = mapPartitions(it => Iterator(aggregatePartition(it)))
      var numPartitions = partiallyAggregated.partitions.length
      val scale = math.max(math.ceil(math.pow(numPartitions, 1.0 / depth)).toInt, 2)
      // If creating an extra level doesn't help reduce
      // the wall-clock time, we stop tree aggregation.
      //如果创建一个额外的级别没有帮助减少
      //挂钟时间,我们停止树聚合。
      // Don't trigger TreeAggregation when it doesn't save wall-clock time
      //当不保存挂钟时间时,不要触发TreeAggregation
      while (numPartitions > scale + math.ceil(numPartitions.toDouble / scale)) {
        numPartitions /= scale
        val curNumPartitions = numPartitions
        partiallyAggregated = partiallyAggregated.mapPartitionsWithIndex {
          (i, iter) => iter.map((i % curNumPartitions, _))
        }.reduceByKey(new HashPartitioner(curNumPartitions), cleanCombOp).values
      }
      partiallyAggregated.reduce(cleanCombOp)
    }
  }

  /**
   * Return the number of elements in the RDD.
   * 返回RDD中的元素数量。
   */
  def count(): Long = sc.runJob(this, Utils.getIteratorSize _).sum

  /**
   * :: Experimental ::
   * Approximate version of count() that returns a potentially incomplete result
   * within a timeout, even if not all tasks have finished.
    * 即使没有完成所有任务，count（）的近似版本也会在超时时间内返回可能不完整的结果。
   */
  @Experimental
  def countApprox(
    timeout: Long,
    confidence: Double = 0.95): PartialResult[BoundedDouble] = withScope {
    val countElements: (TaskContext, Iterator[T]) => Long = { (ctx, iter) =>
      var result = 0L
      while (iter.hasNext) {
        result += 1L
        iter.next()
      }
      result
    }
    val evaluator = new CountEvaluator(partitions.length, confidence)
    sc.runApproximateJob(this, countElements, evaluator, timeout)
  }

  /**
   * Return the count of each unique value in this RDD as a local map of (value, count) pairs.
   * 将此RDD中每个唯一值的计数作为（value，计数）,返回本地Map。 返回 各元素在RDD中出现次数
   * Note that this method should only be used if the resulting map is expected to be small, as
   * the whole thing is loaded into the driver's memory.
    * 请注意，只有在生成的映射预期为小时才应使用此方法整个事情被加载到driver的内存中。
   * To handle very large results, consider using rdd.map(x =&gt; (x, 1L)).reduceByKey(_ + _), which
   * returns an RDD[T, Long] instead of a map.
    * 要处理非常大的结果，请考虑使用rdd.map（x =＆gt;（x，1L））。reduceByKey（_ + _），其中返回RDD [T，Long]而不是Map。
   */
  def countByValue()(implicit ord: Ordering[T] = null): Map[T, Long] = withScope {
    map(value => (value, null)).countByKey()
  }

  /**
   * :: Experimental ::
   * Approximate version of countByValue().
    * countByValue（）的近似版本
   */
  @Experimental
  def countByValueApprox(timeout: Long, confidence: Double = 0.95)(implicit ord: Ordering[T] = null): PartialResult[Map[T, BoundedDouble]] = withScope {
    if (elementClassTag.runtimeClass.isArray) {
      throw new SparkException("countByValueApprox() does not support arrays")
    }
    val countPartition: (TaskContext, Iterator[T]) => OpenHashMap[T, Long] = { (ctx, iter) =>
      val map = new OpenHashMap[T, Long]
      iter.foreach {
        t => map.changeValue(t, 1L, _ + 1L)
      }
      map
    }
    val evaluator = new GroupedCountEvaluator[T](partitions.length, confidence)
    sc.runApproximateJob(this, countPartition, evaluator, timeout)
  }

  /**
   * :: Experimental ::
   * Return approximate number of distinct elements in the RDD.
    * 返回RDD中不同元素的大致数量
   *
   * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
    * 所使用的算法是基于streamlib实现的“HyperLogLog in Practice：
   * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
   * <a href="http://dx.doi.org/10.1145/2452376.2452456">here</a>.
   *
   * The relative accuracy is approximately `1.054 / sqrt(2^p)`. Setting a nonzero `sp &gt; p`
   * would trigger sparse representation of registers, which may reduce the memory consumption
   * and increase accuracy when the cardinality is small.
   *
   * @param p The precision value for the normal set.
   *          `p` must be a value between 4 and `sp` if `sp` is not zero (32 max).
   * @param sp The precision value for the sparse set, between 0 and 32.
   *           If `sp` equals 0, the sparse representation is skipped.
   */
  @Experimental
  def countApproxDistinct(p: Int, sp: Int): Long = withScope {
    require(p >= 4, s"p ($p) must be >= 4")
    require(sp <= 32, s"sp ($sp) must be <= 32")
    require(sp == 0 || p <= sp, s"p ($p) cannot be greater than sp ($sp)")
    val zeroCounter = new HyperLogLogPlus(p, sp)
    aggregate(zeroCounter)(
      (hll: HyperLogLogPlus, v: T) => {
        hll.offer(v)
        hll
      },
      (h1: HyperLogLogPlus, h2: HyperLogLogPlus) => {
        h1.addAll(h2)
        h1
      }).cardinality()
  }

  /**
   * Return approximate number of distinct elements in the RDD.
   * 返回RDD中不同元素的大致数量。
   * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
   * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
   * <a href="http://dx.doi.org/10.1145/2452376.2452456">here</a>.
   *
   * @param relativeSD Relative accuracy. Smaller values create counters that require more space.
   *                   It must be greater than 0.000017.
   */
  def countApproxDistinct(relativeSD: Double = 0.05): Long = withScope {
    require(relativeSD > 0.000017, s"accuracy ($relativeSD) must be greater than 0.000017")
    val p = math.ceil(2.0 * math.log(1.054 / relativeSD) / math.log(2)).toInt
    countApproxDistinct(if (p < 4) 4 else p, 0)
  }

  /**
   * Zips this RDD with its element indices. The ordering is first based on the partition index
   * and then the ordering of items within each partition. So the first item in the first
   * partition gets index 0, and the last item in the last partition receives the largest index.
    *
    * 使用其元素索引将此RDD压缩,排序首先基于分区索引,然后根据每个分区中的项目顺序,因此,第一个分区中的第一个项目获取索引0,
    * 最后一个分区中的最后一个项目将收到最大的索引
   *
   * This is similar to Scala's zipWithIndex but it uses Long instead of Int as the index type.
   * This method needs to trigger a spark job when this RDD contains more than one partitions.
    *
    * 这与Scala的zipWithIndex类似,但它使用Long而不是Int作为索引类型,当此RDD包含多个分区时,此方法需要触发Spark作业。
   *
   * Note that some RDDs, such as those returned by groupBy(), do not guarantee order of
   * elements in a partition. The index assigned to each element is therefore not guaranteed,
   * and may even change if the RDD is reevaluated. If a fixed ordering is required to guarantee
   * the same index assignments, you should sort the RDD with sortByKey() or save it to a file.
    * 请注意，某些RDD（例如由groupBy（）返回的RDD）不保证顺序分区中的元素 因此,分配给每个元素的索引不被保证,
    * 如果RDD被重新评估,甚至可能会改变,如果需要固定的顺序来保证相同的索引分配,则应该使用sortByKey（）对RDD进行排序或将其保存到文件中。
   */
  def zipWithIndex(): RDD[(T, Long)] = withScope {
    new ZippedWithIndexRDD(this)
  }

  /**
   * Zips this RDD with generated unique Long ids. Items in the kth partition will get ids k, n+k,
   * 2*n+k, ..., where n is the number of partitions. So there may exist gaps, but this method
   * won't trigger a spark job, which is different from [[org.apache.spark.rdd.RDD#zipWithIndex]].
    *
   * 拉链这RDD生成唯一的ID,第k个分区中的项目将获得ids k,n+k,2*n+k, ...,其中n是分区的数量,所以可能存在差距,
    * 但这种方法不会触发Spark job,
    *
   * Note that some RDDs, such as those returned by groupBy(), do not guarantee order of
   * elements in a partition. The unique ID assigned to each element is therefore not guaranteed,
   * and may even change if the RDD is reevaluated. If a fixed ordering is required to guarantee
   * the same index assignments, you should sort the RDD with sortByKey() or save it to a file.
    * 请注意,某些RDD(例如由groupBy()返回的RDD)不保证顺序分区中的元素 因此,不保证分配给每个元素的唯一ID,
    * 如果RDD被重新评估,甚至可能会改变,如果需要固定的订购来保证相同的索引分配，您应该使用sortByKey()对RDD进行排序或将其保存到文件中。
   */
  def zipWithUniqueId(): RDD[(T, Long)] = withScope {
    val n = this.partitions.length.toLong
    this.mapPartitionsWithIndex {
      case (k, iter) =>
        iter.zipWithIndex.map {
          case (item, i) =>
            (item, i * n + k)
        }
    }
  }

  /**
   * 
   * 返回一个数组,由数据集的前n个元素组成。注意,这个操作目前并非在多个节点上,并行执行,
   * 而是Driver程序所在机器,单机计算所有的元素(Gateway的内存压力会增大,需要谨慎使用）
   * Take the first num elements of the RDD. It works by first scanning one partition, and use the
   * results from that partition to estimate the number of additional partitions needed to satisfy
   * the limit.
    * 取出RDD的第一个num元素,它首先扫描一个分区,并使用该分区的结果来估计满足限制所需的其他分区数。
   *
   * @note due to complications in the internal implementation, this method will raise
   * an exception if called on an RDD of `Nothing` or `Null`.
   */
  def take(num: Int): Array[T] = withScope {
    if (num == 0) {
      new Array[T](0)
    } else {
      val buf = new ArrayBuffer[T]
      val totalParts = this.partitions.length
      var partsScanned = 0
      while (buf.size < num && partsScanned < totalParts) {
        // The number of partitions to try in this iteration. It is ok for this number to be
        // greater than totalParts because we actually cap it at totalParts in runJob.
        //在此迭代中尝试的分区数,这个数字可以大于totalParts,因为我们实际上将其限制在runJob中的totalParts上。
        var numPartsToTry = 1
        if (partsScanned > 0) {
          // If we didn't find any rows after the previous iteration, quadruple and retry.
          // Otherwise, interpolate the number of partitions we need to try, but overestimate
          // it by 50%. We also cap the estimation in the end.
          //如果在上一次迭代之后我们没有找到任何行，请重新进行四次重试,
          // 否则,插入我们需要尝试的分区数，但高估50％。 我们也最终估计到底。
          if (buf.size == 0) {
            numPartsToTry = partsScanned * 4
          } else {
            // the left side of max is >=1 whenever partsScanned >= 2
            numPartsToTry = Math.max((1.5 * num * partsScanned / buf.size).toInt - partsScanned, 1)
            numPartsToTry = Math.min(numPartsToTry, partsScanned * 4)
          }
        }

        val left = num - buf.size
        val p = partsScanned until math.min(partsScanned + numPartsToTry, totalParts)
        val res = sc.runJob(this, (it: Iterator[T]) => it.take(left).toArray, p)

        res.foreach(buf ++= _.take(num - buf.size))
        partsScanned += numPartsToTry
      }

      buf.toArray
    }
  }

  /**
   * Return the first element in this RDD.
   * 返回RDD数据中第一个数据元素,不排序。
   */
  def first(): T = withScope {
    take(1) match {
      case Array(t) => t
      case _        => throw new UnsupportedOperationException("empty collection")
    }
  }

  /**
   * Returns the top k (largest) elements from this RDD as defined by the specified
   * implicit Ordering[T]. This does the opposite of [[takeOrdered]]. For example:
   * {{{
   * top函数用于从RDD中,按照默认（降序）或者指定的排序规则,返回前num个元素。
   *   sc.parallelize(Seq(10, 4, 2, 12, 3)).top(1)
   *   // returns Array(12)
   *
   *   sc.parallelize(Seq(2, 3, 4, 5, 6)).top(2)
   *   // returns Array(6, 5)
   * }}}
   *
   * @param num k, the number of top elements to return
   * @param ord the implicit ordering for T
   * @return an array of top elements
   */
  def top(num: Int)(implicit ord: Ordering[T]): Array[T] = withScope {
    takeOrdered(num)(ord.reverse)
  }

  /**
   * Returns the first k (smallest) elements from this RDD as defined by the specified
   * implicit Ordering[T] and maintains the ordering. This does the opposite of [[top]].
   * akeOrdered和top类似,只不过以和top相反的顺序返回元素。
   * For example:
   * {{{
   *   sc.parallelize(Seq(10, 4, 2, 12, 3)).takeOrdered(1)
   *   // returns Array(2)
   *
   *   sc.parallelize(Seq(2, 3, 4, 5, 6)).takeOrdered(2)
   *   // returns Array(2, 3)
   * }}}
   *
   * @param num k, the number of elements to return
   * @param ord the implicit ordering for T
   * @return an array of top elements
   */
  def takeOrdered(num: Int)(implicit ord: Ordering[T]): Array[T] = withScope {
    if (num == 0) {
      Array.empty
    } else {
      val mapRDDs = mapPartitions { items =>
        // Priority keeps the largest elements, so let's reverse the ordering.
        //优先级保持最大的元素，所以我们来扭转排序。
        val queue = new BoundedPriorityQueue[T](num)(ord.reverse)
        queue ++= util.collection.Utils.takeOrdered(items, num)(ord)
        Iterator.single(queue)
      }
      if (mapRDDs.partitions.length == 0) {
        Array.empty
      } else {
        mapRDDs.reduce { (queue1, queue2) =>
          queue1 ++= queue2
          queue1
        }.toArray.sorted(ord)
      }
    }
  }

  /**
   * Returns the max of this RDD as defined by the implicit Ordering[T].
    * 返回由隐式排序[T]定义的此RDD的最大值
   * @return the maximum element of the RDD
   */
  def max()(implicit ord: Ordering[T]): T = withScope {
    this.reduce(ord.max)
  }

  /**
   * Returns the min of this RDD as defined by the implicit Ordering[T].
    * 返回由隐式排序[T]定义的此RDD的最小值
   * @return the minimum element of the RDD
   */
  def min()(implicit ord: Ordering[T]): T = withScope {
    this.reduce(ord.min)
  }

  /**
   * @note due to complications in the internal implementation, this method will raise an
   * exception if called on an RDD of `Nothing` or `Null`. This may be come up in practice
   * because, for example, the type of `parallelize(Seq())` is `RDD[Nothing]`.
   * (`parallelize(Seq())` should be avoided anyway in favor of `parallelize(Seq[T]())`.)
   * @return true if and only if the RDD contains no elements at all. Note that an RDD
   *         may be empty even when it has at least 1 partition.
   */
  def isEmpty(): Boolean = withScope {
    partitions.length == 0 || take(1).length == 0
  }

  /**
   * Save this RDD as a text file, using string representations of elements.
   * 将数据集的元素,以textfile的形式,保存到本地文件系统,hdfs或者任何其它hadoop支持的文件系统。
   * Spark将会调用每个元素的toString方法,并将它转换为文件中的一行文本
   */
  def saveAsTextFile(path: String): Unit = withScope {
    // https://issues.apache.org/jira/browse/SPARK-2075
    //
    // NullWritable is a `Comparable` in Hadoop 1.+, so the compiler cannot find an implicit
    // Ordering for it and will use the default `null`. However, it's a `Comparable[NullWritable]`
    // in Hadoop 2.+, so the compiler will call the implicit `Ordering.ordered` method to create an
    // Ordering for `NullWritable`. That's why the compiler will generate different anonymous
    // classes for `saveAsTextFile` in Hadoop 1.+ and Hadoop 2.+.
    //
    // Therefore, here we provide an explicit Ordering `null` to make sure the compiler generate
    // same bytecodes for `saveAsTextFile`.
    val nullWritableClassTag = implicitly[ClassTag[NullWritable]]
    val textClassTag = implicitly[ClassTag[Text]]
    //生成MapPartitionsRDD 
    val r = this.mapPartitions { iter =>
      val text = new Text()
      iter.map { x =>
        text.set(x.toString)
        (NullWritable.get(), text)
      }
    }
    RDD.rddToPairRDDFunctions(r)(nullWritableClassTag, textClassTag, null)
      .saveAsHadoopFile[TextOutputFormat[NullWritable, Text]](path)
  }

  /**
   * Save this RDD as a compressed text file, using string representations of elements.
    * 将此RDD保存为压缩文本文件,使用元素的字符串表示形式。
   */
  def saveAsTextFile(path: String, codec: Class[_ <: CompressionCodec]): Unit = withScope {
    // https://issues.apache.org/jira/browse/SPARK-2075
    val nullWritableClassTag = implicitly[ClassTag[NullWritable]]
    val textClassTag = implicitly[ClassTag[Text]]
    val r = this.mapPartitions { iter =>
      val text = new Text()
      iter.map { x =>
        text.set(x.toString)
        (NullWritable.get(), text)
      }
    }
    RDD.rddToPairRDDFunctions(r)(nullWritableClassTag, textClassTag, null)
      .saveAsHadoopFile[TextOutputFormat[NullWritable, Text]](path, codec)
  }

  /**
   * Save this RDD as a SequenceFile of serialized objects.
    * 将此RDD保存为序列化对象的SequenceFile
   */
  def saveAsObjectFile(path: String): Unit = withScope {
    this.mapPartitions(iter => iter.grouped(10).map(_.toArray))
      .map(x => (NullWritable.get(), new BytesWritable(Utils.serialize(x))))
      .saveAsSequenceFile(path)
  }

  /**
   * Creates tuples of the elements in this RDD by applying `f`.
   * 为RDD中的每个数据单元增加一个key,即生成key-value对。输入的函数是key的构造函数
   * 例如:
   *  val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
	 *	val b = a.keyBy(_.length)
	 *	b.collect
	 *  res26: Array[(Int, String)] = Array((3,dog), (6,salmon), (6,salmon), (3,rat), (8,elephant))
   */
  def keyBy[K](f: T => K): RDD[(K, T)] = withScope {
    val cleanedF = sc.clean(f)
    map(x => (cleanedF(x), x))
  }

  /**
    * A private method for tests, to look at the contents of each partition
    * 一种用于测试的私有方法，以查看每个分区的内容
    * */
  private[spark] def collectPartitions(): Array[Array[T]] = withScope {
    sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
  }

  /**
   * Mark this RDD for checkpointing. It will be saved to a file inside the checkpoint
   * directory set with `SparkContext#setCheckpointDir` and all references to its parent
   * RDDs will be removed. This function must be called before any job has been
   * executed on this RDD. It is strongly recommended that this RDD is persisted in
   * memory, otherwise saving it on a file will require recomputation.
    *
    * 将此RDD标记为检查点。 它将被保存到使用SparkContext＃setCheckpointDir'设置的检查点目录中的文件中，
    * 并且对其父RDD的所有引用将被删除。
    * 在此RDD上执行任何作业之前，必须调用此函数。 强烈建议将此RDD保留在内存中，否则将其保存在文件中将需要重新计算。
   */
  def checkpoint(): Unit = RDDCheckpointData.synchronized {
    // NOTE: we use a global lock here due to complexities downstream with ensuring
    // children RDD partitions point to the correct parent partitions. In the future
    // we should revisit this consideration.
    //注意：由于下游的复杂性,我们使用全局锁定,确保子节点RDD分区指向正确的父分区,今后我们应该重新审视这个考虑。
    if (context.checkpointDir.isEmpty) {
      throw new SparkException("Checkpoint directory has not been set in the SparkContext")
    } else if (checkpointData.isEmpty) {
      checkpointData = Some(new ReliableRDDCheckpointData(this))
    }
  }

  /**
   * Mark this RDD for local checkpointing using Spark's existing caching layer.
   * 使用Spark的现有缓存层将此RDD标记为本地检查点
   * This method is for users who wish to truncate RDD lineages while skipping the expensive
   * step of replicating the materialized data in a reliable distributed file system. This is
   * useful for RDDs with long lineages that need to be truncated periodically (e.g. GraphX).
    *
    * 此方法适用于希望截断RDD谱系的用户,同时跳过在可靠的分布式文件系统中复制实际数据的昂贵步骤,
    * 这对于需要定期截断的长谱系的RDD（例如GraphX）很有用。
   *
   * Local checkpointing sacrifices fault-tolerance for performance. In particular, checkpointed
   * data is written to ephemeral local storage in the executors instead of to a reliable,
   * fault-tolerant storage. The effect is that if an executor fails during the computation,
   * the checkpointed data may no longer be accessible, causing an irrecoverable job failure.
    *
    * 本地检查点牺牲性能的容错性。 特别地，检查点数据被写入执行器中的临时本地存储器，而不是可靠的容错存储器。
    * 效果是，如果执行程序在计算过程中失败，则可能无法访问检查点数据，导致无法恢复的作业失败。
   *
   * This is NOT safe to use with dynamic allocation, which removes executors along
   * with their cached blocks. If you must use both features, you are advised to set
   * `spark.dynamicAllocation.cachedExecutorIdleTimeout` to a high value.
    * 使用动态分配是不安全的，它会删除执行程序与他们的缓存块。 如果您必须同时使用这两个功能，建议您设置
    *`spark.dynamicAllocation.cachedExecutorIdleTimeout`为高值
   *
   * The checkpoint directory set through `SparkContext#setCheckpointDir` is not used.
    * 通sparkContext＃setCheckpointDir设置的检查点目录不被使用。
   */
  def localCheckpoint(): this.type = RDDCheckpointData.synchronized {
    if (conf.getBoolean("spark.dynamicAllocation.enabled", false) &&
      conf.contains("spark.dynamicAllocation.cachedExecutorIdleTimeout")) {
      logWarning("Local checkpointing is NOT safe to use with dynamic allocation, " +
        "which removes executors along with their cached blocks. If you must use both " +
        "features, you are advised to set `spark.dynamicAllocation.cachedExecutorIdleTimeout` " +
        "to a high value. E.g. If you plan to use the RDD for 1 hour, set the timeout to " +
        "at least 1 hour.")
    }

    // Note: At this point we do not actually know whether the user will call persist() on
    // this RDD later, so we must explicitly call it here ourselves to ensure the cached
    // blocks are registered for cleanup later in the SparkContext.

    //注意：在这一点上，我们实际上并不知道用户是否会在此RDD上调用persist()，
    // 所以我们必须在这里自己明确地调用它，以确保缓存的块在SparkContext稍后被注册进行清理。
    //
    // If, however, the user has already called persist() on this RDD, then we must adapt
    // the storage level he/she specified to one that is appropriate for local checkpointing
    // (i.e. uses disk) to guarantee correctness.
    //但是，如果用户已经在此RDD上调用了persist（），那么我们必须进行调整
    //他/她指定的适用于本地检查点的存储级别（即使用磁盘）来保证正确性。

    if (storageLevel == StorageLevel.NONE) {
      persist(LocalRDDCheckpointData.DEFAULT_STORAGE_LEVEL)
    } else {
      persist(LocalRDDCheckpointData.transformStorageLevel(storageLevel), allowOverride = true)
    }

    // If this RDD is already checkpointed and materialized, its lineage is already truncated.
    // We must not override our `checkpointData` in this case because it is needed to recover
    // the checkpointed data. If it is overridden, next time materializing on this RDD will
    // cause error.
    //如果这个RDD已经被检查点和物化了，那么它的谱系已被截断。
    //在这种情况下，我们不能覆盖我们的'checkpointData'，因为它需要恢复检查点数据。 如果它被覆盖，下一次实现这个RDD将会导致错误。
    if (isCheckpointedAndMaterialized) {
      logWarning("Not marking RDD for local checkpoint because it was already " +
        "checkpointed and materialized")
    } else {
      // Lineage is not truncated yet, so just override any existing checkpoint data with ours
      //谱系尚未截断，因此只需覆盖现有的检查点数据
      checkpointData match {
        case Some(_: ReliableRDDCheckpointData[_]) => logWarning(
          "RDD was already marked for reliable checkpointing: overriding with local checkpoint.")
        case _ =>
      }
      checkpointData = Some(new LocalRDDCheckpointData(this))
    }
    this
  }

  /**
   * Return whether this RDD is checkpointed and materialized, either reliably or locally.
   * 返回这个RDD是否被checkpoint了。谨记只有当执行了action操作之后相应的数据才会被checkpoint
   */
  def isCheckpointed: Boolean = checkpointData.exists(_.isCheckpointed)

  /**
   * 
   * Return whether this RDD is checkpointed and materialized, either reliably or locally.
   * This is introduced as an alias for `isCheckpointed` to clarify the semantics of the
   * return value. Exposed for testing.
    * 返回是否可靠地或本地检查点和实现RDD,这是作为`isCheckpointed`的别名引入来澄清的语义返回值,暴露于测试。
   */
  private[spark] def isCheckpointedAndMaterialized: Boolean = isCheckpointed

  /**
   * Return whether this RDD is marked for local checkpointing.
   * 判断这个RDD是否本地检查点
   * Exposed for testing.暴露测试
   */
  private[rdd] def isLocallyCheckpointed: Boolean = {
    checkpointData match {
      case Some(_: LocalRDDCheckpointData[T]) => true
      case _                                  => false
    }
  }

  /**
   * Gets the name of the directory to which this RDD was checkpointed.
   * This is not defined if the RDD is checkpointed locally.
    * 获取此RDD被检查点的目录的名称,如果RDD在本地检查点，则不定义。
   * 获取checkpoint文件,如果RDD没有被checkpoint则返回null
   */
  def getCheckpointFile: Option[String] = {
    checkpointData match {
      case Some(reliable: ReliableRDDCheckpointData[T]) => reliable.getCheckpointDir
      case _ => None
    }
  }

  // =======================================================================
  // Other internal methods and fields
  // =======================================================================

  private var storageLevel: StorageLevel = StorageLevel.NONE

  /** User code that created this RDD (e.g. `textFile`, `parallelize`).
    * 创建此RDD的用户代码（例如`textFile`，`parallelize` */
  @transient private[spark] val creationSite = sc.getCallSite()

  /**
   * The scope associated with the operation that created this RDD.
   * 创建这个RDD操作相关的范围
   * This is more flexible than the call site and can be defined hierarchically. For more
   * detail, see the documentation of {{RDDOperationScope}}. This scope is not defined if the
   * user instantiates this RDD himself without using any Spark operations.
    * 这比呼叫站点更灵活，可以分层定义。 有关详细信息，请参阅{{RDDOperationScope}}的文档。
    * 如果用户在不使用任何Spark操作的情况下自己实例化此RDD，则该范围未定义。
   */
  @transient private[spark] val scope: Option[RDDOperationScope] = {
    Option(sc.getLocalProperty(SparkContext.RDD_SCOPE_KEY)).map(RDDOperationScope.fromJson)
  }

  private[spark] def getCreationSite: String = Option(creationSite).map(_.shortForm).getOrElse("")

  private[spark] def elementClassTag: ClassTag[T] = classTag[T]

  private[spark] var checkpointData: Option[RDDCheckpointData[T]] = None

  /** Returns the first parent RDD */
  //依赖第一个父RDD,在获取first parent的时候,子类经常会使用下面这个方法
  //Seq里的第一个dependency应该是直接的parent,从而从第一个dependency类里获得了rdd,这个rdd就是父RDD
  protected[spark] def firstParent[U: ClassTag]: RDD[U] = {
    dependencies.head.rdd.asInstanceOf[RDD[U]]
  }

  /** 
   *  Returns the jth parent RDD: e.g. rdd.parent[T](0) is equivalent to rdd.firstParent[T] 
   *  返回两父RDD
    *  返回第j个父RDD：例如 rdd.parent [T]（0）相当于rdd.firstParent [T]
   *  */
  protected[spark] def parent[U: ClassTag](j: Int) = {
    dependencies(j).rdd.asInstanceOf[RDD[U]]
  }

  /** The [[org.apache.spark.SparkContext]] that this RDD was created on.
    * 这个RDD创建的[[org.apache.spark.SparkContext]]*/
  def context: SparkContext = sc

  /**
   * Private API for changing an RDD's ClassTag.
   * Used for internal Java-Scala API compatibility.
    * 专用API用于更改RDD的ClassTag,用于内部Java-Scala API兼容性。
   */
  private[spark] def retag(cls: Class[T]): RDD[T] = {
    val classTag: ClassTag[T] = ClassTag.apply(cls)
    this.retag(classTag)
  }

  /**
   * Private API for changing an RDD's ClassTag.
    * 专用API用于更改RDD的ClassTag,用于内部Java-Scala API兼容性
   * Used for internal Java-Scala API compatibility.
   */
  private[spark] def retag(implicit classTag: ClassTag[T]): RDD[T] = {
    this.mapPartitions(identity, preservesPartitioning = true)(classTag)
  }

  // Avoid handling doCheckpoint multiple times to prevent excessive recursion
  //避免多次处理检查点防止递归
  @transient private var doCheckpointCalled = false

  /**
   * 首先保存这个检查点,然后启动一个新的Job来计算,将计算结果写入新创建的目录,所以RDD可能物化存储在内存中
   * Performs the checkpointing of this RDD by saving this. It is called after a job using this RDD
   * has completed (therefore the RDD has been materialized and potentially stored in memory).
   * 递归调用父类doCheckpoint()
   * doCheckpoint() is called recursively on the parent RDDs.
   */
  private[spark] def doCheckpoint(): Unit = {
    RDDOperationScope.withScope(sc, "checkpoint", allowNesting = false, ignoreParent = true) {
      if (!doCheckpointCalled) {
        doCheckpointCalled = true
        if (checkpointData.isDefined) {//如果可选值是 Some 的实例返回 true,否则返回 false
          checkpointData.get.checkpoint()
        } else {
          dependencies.foreach(_.rdd.doCheckpoint())
        }
      }
    }
  }

  /**
   * Changes the dependencies of this RDD from its original parents to a new RDD (`newRDD`)
   * created from the checkpoint file, and forget its old dependencies and partitions.
    * 将此RDD的依赖关系从其原始父项更改为新的RDD（'newRDD'）,从检查点文件创建,并清除原始RDD和分区的依赖关系。
   */
  private[spark] def markCheckpointed(): Unit = {
    clearDependencies()
    partitions_ = null
    //忘记依赖项的构造函数参数
    deps = null // Forget the constructor argument for dependencies too
  }

  /**
   * 清除RDD依赖,垃圾回收集确保删除原始父RDD引用
    * 清除此RDD的依赖关系,此方法必须确保所有引用将原始父类RDD删除，以使父RDD成为垃圾收集,RDD的子类可以覆盖此方法来实现自己的清理
   * Clears the dependencies of this RDD. This method must ensure that all references
   * to the original parent RDDs is removed to enable the parent RDDs to be garbage
   * collected. Subclasses of RDD may override this method for implementing their own cleaning
   * logic. See [[org.apache.spark.rdd.UnionRDD]] for an example.
   */
  protected def clearDependencies() {
    dependencies_ = null
  }

  /**
    * A description of this RDD and its recursive dependencies for debugging.
    * 此RDD的描述及其调试的递归依赖关系
    * */
  def toDebugString: String = {
    // Get a debug description of an rdd without its children
    // 获取没有子的rdd的调试描述
    def debugSelf(rdd: RDD[_]): Seq[String] = {
      import Utils.bytesToString

      val persistence = if (storageLevel != StorageLevel.NONE) storageLevel.description else ""
      val storageInfo = rdd.context.getRDDStorageInfo.filter(_.id == rdd.id).map(info =>
        "    CachedPartitions: %d; MemorySize: %s; ExternalBlockStoreSize: %s; DiskSize: %s".format(
          info.numCachedPartitions, bytesToString(info.memSize),
          //扩展块存储大小
          bytesToString(info.externalBlockStoreSize), bytesToString(info.diskSize)))

      s"$rdd [$persistence]" +: storageInfo
    }

    // Apply a different rule to the last child
    //对最后一个子应用不同的规则
    def debugChildren(rdd: RDD[_], prefix: String): Seq[String] = {
      val len = rdd.dependencies.length
      len match {
        case 0 => Seq.empty
        case 1 =>
          val d = rdd.dependencies.head
          debugString(d.rdd, prefix, d.isInstanceOf[ShuffleDependency[_, _, _]], true)
        case _ =>
          val frontDeps = rdd.dependencies.take(len - 1)
          val frontDepStrings = frontDeps.flatMap(
            d => debugString(d.rdd, prefix, d.isInstanceOf[ShuffleDependency[_, _, _]]))

          val lastDep = rdd.dependencies.last
          val lastDepStrings =
            debugString(lastDep.rdd, prefix, lastDep.isInstanceOf[ShuffleDependency[_, _, _]], true)

          (frontDepStrings ++ lastDepStrings)
      }
    }
    // The first RDD in the dependency stack has no parents, so no need for a +-
    //依赖栈中的第一个RDD没有父进程，所以不需要一个+
    def firstDebugString(rdd: RDD[_]): Seq[String] = {
      val partitionStr = "(" + rdd.partitions.length + ")"
      val leftOffset = (partitionStr.length - 1) / 2
      val nextPrefix = (" " * leftOffset) + "|" + (" " * (partitionStr.length - leftOffset))

      debugSelf(rdd).zipWithIndex.map {
        case (desc: String, 0) => s"$partitionStr $desc"
        case (desc: String, _) => s"$nextPrefix $desc"
      } ++ debugChildren(rdd, nextPrefix)
    }
    def shuffleDebugString(rdd: RDD[_], prefix: String = "", isLastChild: Boolean): Seq[String] = {
      val partitionStr = "(" + rdd.partitions.length + ")"
      val leftOffset = (partitionStr.length - 1) / 2
      val thisPrefix = prefix.replaceAll("\\|\\s+$", "")
      val nextPrefix = (
        thisPrefix
        + (if (isLastChild) "  " else "| ")
        + (" " * leftOffset) + "|" + (" " * (partitionStr.length - leftOffset)))

      debugSelf(rdd).zipWithIndex.map {
        case (desc: String, 0) => s"$thisPrefix+-$partitionStr $desc"
        case (desc: String, _) => s"$nextPrefix$desc"
      } ++ debugChildren(rdd, nextPrefix)
    }
    def debugString(
      rdd: RDD[_],
      prefix: String = "",
      isShuffle: Boolean = true,
      isLastChild: Boolean = false): Seq[String] = {
      if (isShuffle) {
        shuffleDebugString(rdd, prefix, isLastChild)
      } else {
        debugSelf(rdd).map(prefix + _) ++ debugChildren(rdd, prefix)
      }
    }
    firstDebugString(this).mkString("\n")
  }

  override def toString: String = "%s%s[%d] at %s".format(
    Option(name).map(_ + " ").getOrElse(""), getClass.getSimpleName, id, getCreationSite)

  def toJavaRDD(): JavaRDD[T] = {
    new JavaRDD(this)(elementClassTag)
  }
}

/**
 * Defines implicit functions that provide extra functionalities on RDDs of specific types.
 * 定义隐式函数，为特定类型的RDD提供额外的功能
 * For example, [[RDD.rddToPairRDDFunctions]] converts an RDD into a [[PairRDDFunctions]] for
 * key-value-pair RDDs, and enabling extra functionalities such as [[PairRDDFunctions.reduceByKey]].
 */
object RDD {

  // The following implicit functions were in SparkContext before 1.3 and users had to
  // `import SparkContext._` to enable them. Now we move them here to make the compiler find
  // them automatically. However, we still keep the old functions in SparkContext for backward
  // compatibility and forward to the following functions directly.
  //隐式转换,通过隐式转换使kv形式的RDD具有这个类中的方法,一定要是RDD[(K,V)]型的才可以被转换 
  implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)])(implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairRDDFunctions[K, V] = {
    new PairRDDFunctions(rdd)
  }

  implicit def rddToAsyncRDDActions[T: ClassTag](rdd: RDD[T]): AsyncRDDActions[T] = {
    new AsyncRDDActions(rdd)
  }

  implicit def rddToSequenceFileRDDFunctions[K, V](rdd: RDD[(K, V)])(implicit kt: ClassTag[K], vt: ClassTag[V],
    keyWritableFactory: WritableFactory[K],
    valueWritableFactory: WritableFactory[V]): SequenceFileRDDFunctions[K, V] = {
    implicit val keyConverter = keyWritableFactory.convert
    implicit val valueConverter = valueWritableFactory.convert
    new SequenceFileRDDFunctions(rdd,
      keyWritableFactory.writableClass(kt), valueWritableFactory.writableClass(vt))
  }

  implicit def rddToOrderedRDDFunctions[K: Ordering: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): OrderedRDDFunctions[K, V, (K, V)] = {
    new OrderedRDDFunctions[K, V, (K, V)](rdd)
  }

  implicit def doubleRDDToDoubleRDDFunctions(rdd: RDD[Double]): DoubleRDDFunctions = {
    new DoubleRDDFunctions(rdd)
  }

  implicit def numericRDDToDoubleRDDFunctions[T](rdd: RDD[T])(implicit num: Numeric[T]): DoubleRDDFunctions = {
    new DoubleRDDFunctions(rdd.map(x => num.toDouble(x)))
  }
}
