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

import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util.{Date, HashMap => JHashMap}

import scala.collection.{Map, mutable}
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.DynamicVariable

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapred.{FileOutputCommitter, FileOutputFormat, JobConf, OutputFormat}
import org.apache.hadoop.mapreduce.{Job => NewAPIHadoopJob, OutputFormat => NewOutputFormat,
  RecordWriter => NewRecordWriter}

import org.apache.spark._
import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.annotation.Experimental
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.{DataWriteMethod, OutputMetrics}
import org.apache.spark.mapreduce.SparkHadoopMapReduceUtil
import org.apache.spark.partial.{BoundedDouble, PartialResult}
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.{SerializableConfiguration, Utils}
import org.apache.spark.util.collection.CompactBuffer
import org.apache.spark.util.random.StratifiedSamplingUtils

/**
 * Extra functions available on RDDs of (key, value) pairs through an implicit conversion.
 * 扩展类中的方法输入的数据单元是一个包含两个元素的tuple结构,其中第一个元素当成key,第二个当成value
 * PairRDDFunctions主要是Key/Value对操作
 */
class PairRDDFunctions[K, V](self: RDD[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
  extends Logging
  with SparkHadoopMapReduceUtil
  with Serializable
{
  /**
   * Generic function to combine the elements for each key using a custom set of aggregation
   * functions. Turns an RDD[(K, V)] into a result of type RDD[(K, C)], for a "combined type" C
   * Note that V and C can be different -- for example, one might group an RDD of type
   * (Int, Int) into an RDD of type (Int, Seq[Int]). Users provide three functions:
    *
    * 使用一组自定义聚合函数来组合每个键的元素的通用功能。 将RDD [（K，V）]转换为类型RDD [（K，C）]的结果，
    * 对于“组合类型”C注意，V和C可以不同
    * - 例如，可以将RDD 类型（Int，Int）转换为类型（Int，Seq [Int]）的RDD。 用户提供三个功能：
   * combineByKey属于Key-Value型算子,做的是聚集操作,这种变换不会触发作业的提交
   * - `createCombiner`, which turns a V into a C (e.g., creates a one-element list)
    *                   其将V变成C（例如，创建单元列表）
   * - `mergeValue`, to merge a V into a C (e.g., adds it to the end of a list)
    *               将V合并成C（例如，将其添加到列表的末尾）
   * - `mergeCombiners`, to combine two C's into a single one.将两个C组合成一个
   *
   * In addition, users can control the partitioning of the output RDD, and whether to perform
   * map-side aggregation (if a mapper can produce multiple items with the same key).
   * 在一个由（K,V）对组成的数据集上调用,返回一个（K,Seq[V])对的数据集。
   * 注意：默认情况下,使用8个并行任务进行分组,你可以传入numTask可选参数,根据数据量设置不同数目的Task
   * 1)创建Aggregator(其中mergeValue)
   * 
   */
  def combineByKey[C](createCombiner: V => C,//一个组合函数,用于将RDD[K,V]中的V转换成一个新的值C1
      mergeValue: (C, V) => C,//合并值函数,将一个C1类型值和一个V类型值合并成一个C2类型,输入参数为(C1,V),输出为新的C2
      mergeCombiners: (C, C) => C,//该函数把2个元素集合C合并
      partitioner: Partitioner,  
      mapSideCombine: Boolean = true,//是否需要在worker端进行combine操作
      serializer: Serializer = null): RDD[(K, C)] = self.withScope {
    require(mergeCombiners != null, "mergeCombiners must be defined") // required as of Spark 0.9.0
    if (keyClass.isArray) {//key是数组时需要特殊的partitioner,默认的HashPartitioner不支持数组 
      if (mapSideCombine) {//是否需要在worker端进行combine操作
        throw new SparkException("Cannot use map-side combining with array keys.")
      }
      if (partitioner.isInstanceOf[HashPartitioner]) {
        throw new SparkException("Default partitioner cannot partition array keys.")
      }
    }
    //创建Aggregator(其中mergeValue)
    val aggregator = new Aggregator[K, V, C](
      self.context.clean(createCombiner),
      self.context.clean(mergeValue),
      self.context.clean(mergeCombiners))
    if (self.partitioner == Some(partitioner)) {
    //一般的RDD的partitioner是None,这个条件不成立,即使成立只需要对这个数据做一次按key合并value的操作即可
      self.mapPartitions(iter => {
        val context = TaskContext.get()
        new InterruptibleIterator(context, aggregator.combineValuesByKey(iter, context))
      }, preservesPartitioning = true)
    } else {
      //进行shuffle过程对key进行分组 
      new ShuffledRDD[K, V, C](self, partitioner)
        .setSerializer(serializer)
        .setAggregator(aggregator)
        .setMapSideCombine(mapSideCombine)//是否需要在worker端进行combine操作
    }
  }

  /**
   * Simplified version of combineByKey that hash-partitions the output RDD.
    * combineByKey的简化版本,用于散列分区输出RDD
   */
  def combineByKey[C](createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      numPartitions: Int): RDD[(K, C)] = self.withScope {
    combineByKey(createCombiner, mergeValue, mergeCombiners, new HashPartitioner(numPartitions))
  }

  /**
   * Aggregate the values of each key, using given combine functions and a neutral "zero value".
   * This function can return a different result type, U, than the type of the values in this RDD,
   * V. Thus, we need one operation for merging a V into a U and one operation for merging two U's,
   * as in scala.TraversableOnce. The former operation is used for merging values within a
   * partition, and the latter is used for merging values between partitions. To avoid memory
   * allocation, both of these functions are allowed to modify and return their first argument
   * instead of creating a new U.
    * 使用给定的组合函数和中性“零值”来聚合每个键的值。
    *此函数可以返回与此RDD中的值类型不同的结果类型U，
    *因此，我们需要一个操作来将V合并成一个U和一个操作来合并两个U，
    *如在scala.TraversableOnce。 前一个操作用于合并一个值中的值
    *分区，后者用于在分区之间合并值。 避免记忆
    *分配，这两个函数都允许修改并返回其第一个参数
    *而不是创建一个新的U.
   */
  def aggregateByKey[U: ClassTag](zeroValue: U, partitioner: Partitioner)(seqOp: (U, V) => U,
      combOp: (U, U) => U): RDD[(K, U)] = self.withScope {
    // Serialize the zero value to a byte array so that we can get a new clone of it on each key
    //将零值序列化为字节数组，以便我们可以在每个键上获得一个新的克隆
    val zeroBuffer = SparkEnv.get.serializer.newInstance().serialize(zeroValue)
    val zeroArray = new Array[Byte](zeroBuffer.limit)
    zeroBuffer.get(zeroArray)

    lazy val cachedSerializer = SparkEnv.get.serializer.newInstance()
    val createZero = () => cachedSerializer.deserialize[U](ByteBuffer.wrap(zeroArray))

    // We will clean the combiner closure later in `combineByKey`
    //我们将在`combineByKey`中稍后清理组合器
    val cleanedSeqOp = self.context.clean(seqOp)
    combineByKey[U]((v: V) => cleanedSeqOp(createZero(), v), cleanedSeqOp, combOp, partitioner)
  }

  /**
   * Aggregate the values of each key, using given combine functions and a neutral "zero value".
   * This function can return a different result type, U, than the type of the values in this RDD,
   * V. Thus, we need one operation for merging a V into a U and one operation for merging two U's,
   * as in scala.TraversableOnce. The former operation is used for merging values within a
   * partition, and the latter is used for merging values between partitions. To avoid memory
   * allocation, both of these functions are allowed to modify and return their first argument
   * instead of creating a new U.
    * 使用给定的组合函数和中性“零值”来聚合每个键的值。
    *此函数可以返回与此RDD中的值类型不同的结果类型U，
    *因此，我们需要一个操作来将V合并成一个U和一个操作来合并两个U，
    *如在scala.TraversableOnce。 前一个操作用于合并一个值中的值
    *分区，后者用于在分区之间合并值。 避免记忆
    *分配，这两个函数都允许修改并返回其第一个参数
    *而不是创建一个新的U.
   */
  def aggregateByKey[U: ClassTag](zeroValue: U, numPartitions: Int)(seqOp: (U, V) => U,
      combOp: (U, U) => U): RDD[(K, U)] = self.withScope {
    aggregateByKey(zeroValue, new HashPartitioner(numPartitions))(seqOp, combOp)
  }

  /**
   * Aggregate the values of each key, using given combine functions and a neutral "zero value".
   * This function can return a different result type, U, than the type of the values in this RDD,
   * V. Thus, we need one operation for merging a V into a U and one operation for merging two U's,
   * as in scala.TraversableOnce. The former operation is used for merging values within a
   * partition, and the latter is used for merging values between partitions. To avoid memory
   * allocation, both of these functions are allowed to modify and return their first argument
   * instead of creating a new U.
    * 使用给定的组合函数和中性“零值”来聚合每个键的值。
    *此函数可以返回与此RDD中的值类型不同的结果类型U，
    *因此，我们需要一个操作来将V合并成一个U和一个操作来合并两个U，
    *如在scala.TraversableOnce。 前一个操作用于合并一个值中的值
    *分区，后者用于在分区之间合并值。 避免记忆
    *分配，这两个函数都允许修改并返回其第一个参数
    *而不是创建一个新的U.
   */
  def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,
      combOp: (U, U) => U): RDD[(K, U)] = self.withScope {
    aggregateByKey(zeroValue, defaultPartitioner(self))(seqOp, combOp)
  }

  /**
   * Merge the values for each key using an associative function and a neutral "zero value" which
   * may be added to the result an arbitrary number of times, and must not change the result
   * (e.g., Nil for list concatenation, 0 for addition, or 1 for multiplication.).
    * 使用关联函数和中性“零值”合并每个键的值，可以将任意次数添加到结果中，并且不得更改结果（例如，列表连接为零，添加为0或为1 用于乘法）
   */
  def foldByKey(
      zeroValue: V,
      partitioner: Partitioner)(func: (V, V) => V): RDD[(K, V)] = self.withScope {
    // Serialize the zero value to a byte array so that we can get a new clone of it on each key
    //将零值序列化为字节数组，以便我们可以在每个键上获得一个新的克隆
    val zeroBuffer = SparkEnv.get.serializer.newInstance().serialize(zeroValue)
    val zeroArray = new Array[Byte](zeroBuffer.limit)
    zeroBuffer.get(zeroArray)

    // When deserializing, use a lazy val to create just one instance of the serializer per task
    //反序列化时,使用一个惰性值来创建每个任务的序列化程序的一个实例
    lazy val cachedSerializer = SparkEnv.get.serializer.newInstance()
    val createZero = () => cachedSerializer.deserialize[V](ByteBuffer.wrap(zeroArray))

    val cleanedFunc = self.context.clean(func)
    combineByKey[V]((v: V) => cleanedFunc(createZero(), v), cleanedFunc, cleanedFunc, partitioner)
  }

  /**
   * Merge the values for each key using an associative function and a neutral "zero value" which
   * may be added to the result an arbitrary number of times, and must not change the result
   * (e.g., Nil for list concatenation, 0 for addition, or 1 for multiplication.).
    * 使用关联函数和中性“零值”合并每个键的值，可以将任意次数添加到结果中，并且不得更改结果（例如，列表连接为零，添加为0或为1 用于乘法）。
   */
  def foldByKey(zeroValue: V, numPartitions: Int)(func: (V, V) => V): RDD[(K, V)] = self.withScope {
    foldByKey(zeroValue, new HashPartitioner(numPartitions))(func)
  }

  /**
   * Merge the values for each key using an associative function and a neutral "zero value" which
   * may be added to the result an arbitrary number of times, and must not change the result
   * (e.g., Nil for list concatenation, 0 for addition, or 1 for multiplication.).
    * 使用关联函数和中性“零值”合并每个键的值，可以将任意次数添加到结果中，并且不得更改结果（例如，列表连接为零，添加为0或为1 用于乘法）。
   */
  def foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)] = self.withScope {
    foldByKey(zeroValue, defaultPartitioner(self))(func)
  }

  /**
   * Return a subset of this RDD sampled by key (via stratified sampling).
    * 返回通过键采样的该RDD的子集（通过分层采样）
   *
   * Create a sample of this RDD using variable sampling rates for different keys as specified by
   * `fractions`, a key to sampling rate map, via simple random sampling with one pass over the
   * RDD, to produce a sample of size that's approximately equal to the sum of
   * math.ceil(numItems * samplingRate) over all key values.
   *
    * 使用如指定的不同键的可变采样率创建此RDD的样本
    *“分数”，采样率图的关键，通过简单的随机抽样，一次通过
    * RDD，以产生大约等于总和大小的样本
    *所有键值上的math.ceil（numItems * samplingRate）。
   * @param withReplacement whether to sample with or without replacement
   * @param fractions map of specific keys to sampling rates
   * @param seed seed for the random number generator
   * @return RDD containing the sampled subset
   */
  def sampleByKey(withReplacement: Boolean,
      fractions: Map[K, Double],
      seed: Long = Utils.random.nextLong): RDD[(K, V)] = self.withScope {

    require(fractions.values.forall(v => v >= 0.0), "Negative sampling rates.")

    val samplingFunc = if (withReplacement) {
      StratifiedSamplingUtils.getPoissonSamplingFunction(self, fractions, false, seed)
    } else {
      StratifiedSamplingUtils.getBernoulliSamplingFunction(self, fractions, false, seed)
    }
    self.mapPartitionsWithIndex(samplingFunc, preservesPartitioning = true)
  }

  /**
   * ::Experimental::
   * Return a subset of this RDD sampled by key (via stratified sampling) containing exactly
   * math.ceil(numItems * samplingRate) for each stratum (group of pairs with the same key).
    * 通过键（通过分层采样）返回包含正确包含的RDD采样子集
    *每个层的math.ceil（numItems * samplingRate）（具有相同键的成对组）。
   *
   * This method differs from [[sampleByKey]] in that we make additional passes over the RDD to
   * create a sample size that's exactly equal to the sum of math.ceil(numItems * samplingRate)
   * over all key values with a 99.99% confidence. When sampling without replacement, we need one
   * additional pass over the RDD to guarantee sample size; when sampling with replacement, we need
   * two additional passes.
    *
    * 该方法与[[sampleByKey]]不同，因为我们对RDD进行了额外的传递
    *创建一个完全等于math.ceil（numItems * samplingRate）的总和的样本大小，
    *所有关键值都有99.99％的置信度。 抽样时不需要更换，我们需要一个
    *额外通过RDD以保证样本量; 当需要更换时，我们需要
    *另外两次通行证。
   *
   * @param withReplacement whether to sample with or without replacement
   * @param fractions map of specific keys to sampling rates
   * @param seed seed for the random number generator
   * @return RDD containing the sampled subset
   */
  @Experimental
  def sampleByKeyExact(
      withReplacement: Boolean,
      fractions: Map[K, Double],
      seed: Long = Utils.random.nextLong): RDD[(K, V)] = self.withScope {

    require(fractions.values.forall(v => v >= 0.0), "Negative sampling rates.")

    val samplingFunc = if (withReplacement) {
      StratifiedSamplingUtils.getPoissonSamplingFunction(self, fractions, true, seed)
    } else {
      StratifiedSamplingUtils.getBernoulliSamplingFunction(self, fractions, true, seed)
    }
    self.mapPartitionsWithIndex(samplingFunc, preservesPartitioning = true)
  }

  /**
   * Merge the values for each key using an associative reduce function. This will also perform
   * the merging locally on each mapper before sending results to a reducer, similarly to a
   * "combiner" in MapReduce.
    * 使用关联缩减功能合并每个键的值。 在将映射结果发送给reducer之前，这也将在每个映射器上执行本地合并，类似于MapReduce中的“组合器”。
   * 
   * 该函数用于将RDD[K,V]中每个K对应的V值根据映射函数来运算
   * 参数numPartitions用于指定分区数
   * 参数partitioner用于指定分区
   * 例如:
   * scala> var rdd1 = sc.makeRDD(Array(("A",0),("A",2),("B",1),("B",2),("C",1)))
   * scala> var rdd2 = rdd1.reduceByKey((x,y) => x + y)
   * scala> rdd2.collect
   * res85: Array[(String, Int)] = Array((A,2), (B,3), (C,1))
   */
  def reduceByKey(partitioner: Partitioner, func: (V, V) => V): RDD[(K, V)] = self.withScope {
    combineByKey[V]((v: V) => v, func, func, partitioner)
  }

  /**
   * 
   * Merge the values for each key using an associative reduce function. This will also perform
   * the merging locally on each mapper before sending results to a reducer, similarly to a
   * "combiner" in MapReduce. Output will be hash-partitioned with numPartitions partitions.
   * 该函数用于将RDD[K,V]中每个K对应的V值根据映射函数来运算
   * 参数numPartitions用于指定分区数
   * 参数partitioner用于指定分区数
   */
  def reduceByKey(func: (V, V) => V, numPartitions: Int): RDD[(K, V)] = self.withScope {
    reduceByKey(new HashPartitioner(numPartitions), func)
  }

  /**
   * Merge the values for each key using an associative reduce function. This will also perform
   * the merging locally on each mapper before sending results to a reducer, similarly to a
   * "combiner" in MapReduce. Output will be hash-partitioned with the existing partitioner/
   * parallelism level.
    * 使用关联缩减功能合并每个键的值。 在将映射结果发送给reducer之前，这也将在每个映射器上执行本地合并，
    * 类似于MapReduce中的“组合器”。 输出将使用现有的分区/并行级别进行散列分区。
   */
  def reduceByKey(func: (V, V) => V): RDD[(K, V)] = self.withScope {
    reduceByKey(defaultPartitioner(self), func)
  }

  /**
   * Merge the values for each key using an associative reduce function, but return the results
   * immediately to the master as a Map. This will also perform the merging locally on each mapper
   * before sending results to a reducer, similarly to a "combiner" in MapReduce.
    *
    * 使用关联缩减函数合并每个键的值，但将结果立即作为Map返回给主。 在将映射结果发送给reducer之前，
    * 这也将在每个映射器上执行本地合并，类似于MapReduce中的“组合器”。
    *
   * 该函数将RDD[K,V]中每个K对应的V值根据映射函数来运算,运算结果映射到一个Map[K,V]中,而不是RDD[K,V]
   * 例如:
   * scala> var rdd1 = sc.makeRDD(Array(("A",0),("A",2),("B",1),("B",2),("C",1)))
   * scala> rdd1.reduceByKeyLocally((x,y) => x + y)
   * res90: scala.collection.Map[String,Int] = Map(B -> 3, A -> 2, C -> 1)
   */
  def reduceByKeyLocally(func: (V, V) => V): Map[K, V] = self.withScope {
    val cleanedF = self.sparkContext.clean(func)

    if (keyClass.isArray) {
      throw new SparkException("reduceByKeyLocally() does not support array keys")
    }

    val reducePartition = (iter: Iterator[(K, V)]) => {
      val map = new JHashMap[K, V]
      iter.foreach { pair =>
        val old = map.get(pair._1)
        map.put(pair._1, if (old == null) pair._2 else cleanedF(old, pair._2))
      }
      Iterator(map)
    } : Iterator[JHashMap[K, V]]

    val mergeMaps = (m1: JHashMap[K, V], m2: JHashMap[K, V]) => {
      m2.foreach { pair =>
        val old = m1.get(pair._1)
        m1.put(pair._1, if (old == null) pair._2 else cleanedF(old, pair._2))
      }
      m1
    } : JHashMap[K, V]

    self.mapPartitions(reducePartition).reduce(mergeMaps)
  }

  /** Alias for reduceByKeyLocally */
  @deprecated("Use reduceByKeyLocally", "1.0.0")
  def reduceByKeyToDriver(func: (V, V) => V): Map[K, V] = self.withScope {
    reduceByKeyLocally(func)
  }

  /**
   * Count the number of elements for each key, collecting the results to a local Map.
   * 用于统计RDD[K,V]中每个K的数量
   * 返回一个(K,Int)对的Map,表示每一个key对应的元素个数
   * 例如:
   * scala> var rdd1 = sc.makeRDD(Array(("A",0),("A",2),("B",1),("B",2),("B",3)))
   * scala> rdd1.countByKey
   * res5: scala.collection.Map[String,Long] = Map(A -> 2, B -> 3)
   *
   * Note that this method should only be used if the resulting map is expected to be small, as
   * the whole thing is loaded into the driver's memory.
   * To handle very large results, consider using rdd.mapValues(_ => 1L).reduceByKey(_ + _), which
   * returns an RDD[T, Long] instead of a map.
   */
  def countByKey(): Map[K, Long] = self.withScope {
    self.mapValues(_ => 1L).reduceByKey(_ + _).collect().toMap
  }

  /**
   * :: Experimental ::
   * Approximate version of countByKey that can return a partial result if it does
   * not finish within a timeout.
    * countByKey的近似版本，如果在超时时间内未完成，则可返回部分结果。
   */
  @Experimental
  def countByKeyApprox(timeout: Long, confidence: Double = 0.95)
      : PartialResult[Map[K, BoundedDouble]] = self.withScope {
    self.map(_._1).countByValueApprox(timeout, confidence)
  }

  /**
   * :: Experimental ::
   *
   * Return approximate number of distinct values for each key in this RDD.
    * 返回此RDD中每个键的不同值的近似数
   *
   * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
   * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
   * <a href="http://dx.doi.org/10.1145/2452376.2452456">here</a>.
   *
   * The relative accuracy is approximately `1.054 / sqrt(2^p)`. Setting a nonzero `sp > p`
   * would trigger sparse representation of registers, which may reduce the memory consumption
   * and increase accuracy when the cardinality is small.
   *
   * @param p The precision value for the normal set.
   *          `p` must be a value between 4 and `sp` if `sp` is not zero (32 max).
   * @param sp The precision value for the sparse set, between 0 and 32.
   *           If `sp` equals 0, the sparse representation is skipped.
   * @param partitioner Partitioner to use for the resulting RDD.
   */
  @Experimental
  def countApproxDistinctByKey(
      p: Int,
      sp: Int,
      partitioner: Partitioner): RDD[(K, Long)] = self.withScope {
    require(p >= 4, s"p ($p) must be >= 4")
    require(sp <= 32, s"sp ($sp) must be <= 32")
    require(sp == 0 || p <= sp, s"p ($p) cannot be greater than sp ($sp)")
    val createHLL = (v: V) => {
      val hll = new HyperLogLogPlus(p, sp)
      hll.offer(v)
      hll
    }
    val mergeValueHLL = (hll: HyperLogLogPlus, v: V) => {
      hll.offer(v)
      hll
    }
    val mergeHLL = (h1: HyperLogLogPlus, h2: HyperLogLogPlus) => {
      h1.addAll(h2)
      h1
    }

    combineByKey(createHLL, mergeValueHLL, mergeHLL, partitioner).mapValues(_.cardinality())
  }

  /**
   * Return approximate number of distinct values for each key in this RDD.
   *  返回此RDD中每个键的不同值的近似数
    *
   * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
   * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
   * <a href="http://dx.doi.org/10.1145/2452376.2452456">here</a>.
   *
   * @param relativeSD Relative accuracy. Smaller values create counters that require more space.
   *                   It must be greater than 0.000017.
   * @param partitioner partitioner of the resulting RDD
   */
  def countApproxDistinctByKey(
      relativeSD: Double,
      partitioner: Partitioner): RDD[(K, Long)] = self.withScope {
    require(relativeSD > 0.000017, s"accuracy ($relativeSD) must be greater than 0.000017")
    val p = math.ceil(2.0 * math.log(1.054 / relativeSD) / math.log(2)).toInt
    assert(p <= 32)
    countApproxDistinctByKey(if (p < 4) 4 else p, 0, partitioner)
  }

  /**
   * Return approximate number of distinct values for each key in this RDD.
   *  返回此RDD中每个键的不同值的近似数
   * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
   * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
   * <a href="http://dx.doi.org/10.1145/2452376.2452456">here</a>.
   *
   * @param relativeSD Relative accuracy. Smaller values create counters that require more space.
   *                   It must be greater than 0.000017.
   * @param numPartitions number of partitions of the resulting RDD
   */
  def countApproxDistinctByKey(
      relativeSD: Double,
      numPartitions: Int): RDD[(K, Long)] = self.withScope {
    countApproxDistinctByKey(relativeSD, new HashPartitioner(numPartitions))
  }

  /**
   * Return approximate number of distinct values for each key in this RDD.
   *返回此RDD中每个键的不同值的近似数
   * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
   * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
   * <a href="http://dx.doi.org/10.1145/2452376.2452456">here</a>.
   *
   * @param relativeSD Relative accuracy. Smaller values create counters that require more space.
   *                   It must be greater than 0.000017.
   */
  def countApproxDistinctByKey(relativeSD: Double = 0.05): RDD[(K, Long)] = self.withScope {
    countApproxDistinctByKey(relativeSD, defaultPartitioner(self))
  }

  /**
   * Group the values for each key in the RDD into a single sequence. Allows controlling the
   * partitioning of the resulting key-value pair RDD by passing a Partitioner.
   * The ordering of elements within each group is not guaranteed, and may even differ
   * each time the resulting RDD is evaluated.
   * 在一个由（K,V）对组成的数据集上调用,返回一个（K,Seq[V])对的数据集。
   * 注意：默认情况下,使用8个并行任务进行分组,你可以传入numTask可选参数,根据数据量设置不同数目的Task 
   * Note: This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using [[PairRDDFunctions.aggregateByKey]]
   * or [[PairRDDFunctions.reduceByKey]] will provide much better performance.
   *
   * Note: As currently implemented, groupByKey must be able to hold all the key-value pairs for any
   * key in memory. If a key has too many values, it can result in an [[OutOfMemoryError]]
   * 
   * 该函数用于将RDD[K,V]中每个K对应的V值,返回一个（K,Seq[V])对的数据集
   * 参数partitioner用于指定分区
   * 例如:
   * scala> var rdd1 = sc.makeRDD(Array(("A",0),("A",2),("B",1),("B",2),("C",1)))
   * scala> rdd1.groupByKey().collect
   * res81: Array[(String, Iterable[Int])] = Array((A,CompactBuffer(0, 2)), (B,CompactBuffer(2, 1)), (C,CompactBuffer(1)))
   */
  def groupByKey(partitioner: Partitioner): RDD[(K, Iterable[V])] = self.withScope {
    // groupByKey shouldn't use map side combine because map side combine does not
    // reduce the amount of data shuffled and requires all map side data be inserted
    // into a hash table, leading to more objects in the old gen.
    val createCombiner = (v: V) => CompactBuffer(v)
    val mergeValue = (buf: CompactBuffer[V], v: V) => buf += v
    val mergeCombiners = (c1: CompactBuffer[V], c2: CompactBuffer[V]) => c1 ++= c2
    val bufs = combineByKey[CompactBuffer[V]](
      createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine = false)
    bufs.asInstanceOf[RDD[(K, Iterable[V])]]
  }

  /**
   * Group the values for each key in the RDD into a single sequence. Hash-partitions the
   * resulting RDD with into `numPartitions` partitions. The ordering of elements within
   * each group is not guaranteed, and may even differ each time the resulting RDD is evaluated.
    *
    * 将RDD中每个键的值分组为单个序列,将生成的RDD哈希分区到“numPartitions”分区中,每个组中的元素的排序不能保证,并且每次得到的RDD被评估时甚至可能不同。
    *
   * 该函数用于将RDD[K,V]中每个K对应的V值,返回一个（K,Seq[V])对的数据集
   * 参数numPartitions用于指定分区数
   * Note: This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using [[PairRDDFunctions.aggregateByKey]]
   * or [[PairRDDFunctions.reduceByKey]] will provide much better performance.
   *
   * Note: As currently implemented, groupByKey must be able to hold all the key-value pairs for any
   * key in memory. If a key has too many values, it can result in an [[OutOfMemoryError]].
   */
  def groupByKey(numPartitions: Int): RDD[(K, Iterable[V])] = self.withScope {
    groupByKey(new HashPartitioner(numPartitions))
  }

  /**
   * Return a copy of the RDD partitioned using the specified partitioner.
   * 该函数根据partitioner函数生成新的ShuffleRDD,将原RDD重新分区
   */
  def partitionBy(partitioner: Partitioner): RDD[(K, V)] = self.withScope {
    if (keyClass.isArray && partitioner.isInstanceOf[HashPartitioner]) {
      throw new SparkException("Default partitioner cannot partition array keys.")
    }
    if (self.partitioner == Some(partitioner)) {
      self
    } else {
      new ShuffledRDD[K, V, V](self, partitioner)
    }
  }

  /**
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Uses the given Partitioner to partition the output RDD.
   * 在类型为（K,V)和（K,W)类型的数据集上调用时,返回一个相同key对应的所有元素对在一起的(K, (V, W))数据集
   * join相当于SQL中的内关联join,只返回两个RDD根据K可以关联上的结果,join只能用于两个RDD之间的关联,如果要多个RDD关联,多关联几次即可
   */
  def join[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (V, W))] = self.withScope {
    this.cogroup(other, partitioner).flatMapValues( pair =>
      for (v <- pair._1.iterator; w <- pair._2.iterator) yield (v, w)
    )
  }

  /**
   * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
   * pair (k, (v, None)) if no elements in `other` have key k. Uses the given Partitioner to
   * partition the output RDD.
   * leftOuterJoin类似于SQL中的左外关联left outer join,返回结果以前面的RDD为主,关联不上的记录为空。
   * 只能用于两个RDD之间的关联,如果要多个RDD关联,多关联几次即可。
   */
  def leftOuterJoin[W](
      other: RDD[(K, W)],
      partitioner: Partitioner): RDD[(K, (V, Option[W]))] = self.withScope {
    this.cogroup(other, partitioner).flatMapValues { pair =>
      if (pair._2.isEmpty) {
        pair._1.iterator.map(v => (v, None))
      } else {
        for (v <- pair._1.iterator; w <- pair._2.iterator) yield (v, Some(w))
      }
    }
  }

  /**
   * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
   * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k. Uses the given Partitioner to
   * partition the output RDD.
   * rightOuterJoin类似于SQL中的有外关联right outer join,返回结果以参数中的RDD为主,关联不上的记录为空。
   * 只能用于两个RDD之间的关联,如果要多个RDD关联,多关联几次即可
   */
  def rightOuterJoin[W](other: RDD[(K, W)], partitioner: Partitioner)
      : RDD[(K, (Option[V], W))] = self.withScope {
    this.cogroup(other, partitioner).flatMapValues { pair =>
      if (pair._1.isEmpty) {
        pair._2.iterator.map(w => (None, w))
      } else {
        for (v <- pair._1.iterator; w <- pair._2.iterator) yield (Some(v), w)
      }
    }
  }

  /**
   * Perform a full outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (Some(v), Some(w))) for w in `other`, or
   * the pair (k, (Some(v), None)) if no elements in `other` have key k. Similarly, for each
   * element (k, w) in `other`, the resulting RDD will either contain all pairs
   * (k, (Some(v), Some(w))) for v in `this`, or the pair (k, (None, Some(w))) if no elements
   * in `this` have key k. Uses the given Partitioner to partition the output RDD.
   */
  def fullOuterJoin[W](other: RDD[(K, W)], partitioner: Partitioner)
      : RDD[(K, (Option[V], Option[W]))] = self.withScope {
    this.cogroup(other, partitioner).flatMapValues {
      case (vs, Seq()) => vs.iterator.map(v => (Some(v), None))
      case (Seq(), ws) => ws.iterator.map(w => (None, Some(w)))
      case (vs, ws) => for (v <- vs.iterator; w <- ws.iterator) yield (Some(v), Some(w))
    }
  }

  /**
   * Simplified version of combineByKey that hash-partitions the resulting RDD using the
   * existing partitioner/parallelism level.
    * combineByKey的简化版，使用现有的分区/并行级别对生成的RDD进行散列分区
   */
  def combineByKey[C](createCombiner: V => C, mergeValue: (C, V) => C, mergeCombiners: (C, C) => C)
    : RDD[(K, C)] = self.withScope {
    combineByKey(createCombiner, mergeValue, mergeCombiners, defaultPartitioner(self))
  }

  /**
   * Group the values for each key in the RDD into a single sequence. Hash-partitions the
   * resulting RDD with the existing partitioner/parallelism level. The ordering of elements
   * within each group is not guaranteed, and may even differ each time the resulting RDD is
   * evaluated.
    *
    * 将RDD中每个键的值分组为单个序列,使用现有的分区/并行级别对结果进行哈希分区,每个组中的元素的排序不能保证,并且每次得到的RDD被评估时甚至可能不同。
   *
   * Note: This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using [[PairRDDFunctions.aggregateByKey]]
   * or [[PairRDDFunctions.reduceByKey]] will provide much better performance.
    *
    * 注意：此操作可能非常昂贵。 如果您正在分组以执行使用[[PairRDDFunctions.aggregateByKey]]或
    * [[PairRDDFunctions.reduceByKey]]在每个键上的聚合（如和或平均值）将提供更好的性能。
   */
  def groupByKey(): RDD[(K, Iterable[V])] = self.withScope {
    groupByKey(defaultPartitioner(self))
  }

  /**
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Performs a hash join across the cluster.
    *
    * 在“this”和“other”中返回一个包含所有匹配键元素的RDD。 每对元素将作为（k，（v1，v2））元组返回，
    * 其中（k，v1）在“this”中，（k，v2）在“other”中。 在集群中执行散列连接。
   */
  def join[W](other: RDD[(K, W)]): RDD[(K, (V, W))] = self.withScope {
    join(other, defaultPartitioner(self, other))
  }

  /**
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Performs a hash join across the cluster.
    * 在“this”和“other”中返回一个包含所有匹配键元素的RDD。 每对元素将作为（k，（v1，v2））元组返回，
    * 其中（k，v1）在“this”中，（k，v2）在“other”中。 在集群中执行散列连接。
   */
  def join[W](other: RDD[(K, W)], numPartitions: Int): RDD[(K, (V, W))] = self.withScope {
    join(other, new HashPartitioner(numPartitions))
  }

  /**
   * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
   * pair (k, (v, None)) if no elements in `other` have key k. Hash-partitions the output
   * using the existing partitioner/parallelism level.
    *
    * 执行“this”和“other”的左外连接。 对于“this”中的每个元素（k，v）
    *得到的RDD将包含“other”中的w的所有对（k，（v，Some（w））），或
    * pair（k，（v，None））如果`other`中的元素没有键k。 散列分区输出
    *使用现有的分区/并行级别。
   */
  def leftOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (V, Option[W]))] = self.withScope {
    leftOuterJoin(other, defaultPartitioner(self, other))
  }

  /**
   * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
   * pair (k, (v, None)) if no elements in `other` have key k. Hash-partitions the output
   * into `numPartitions` partitions.
    * 执行“this”和“other”的左外连接。 对于“this”中的每个元素（k，v）
    *得到的RDD将包含“other”中的w的所有对（k，（v，Some（w））），或
    * pair（k，（v，None））如果`other`中的元素没有键k。 散列分区输出
    *到`numPartition`分区。
   */
  def leftOuterJoin[W](
      other: RDD[(K, W)],
      numPartitions: Int): RDD[(K, (V, Option[W]))] = self.withScope {
    leftOuterJoin(other, new HashPartitioner(numPartitions))
  }

  /**
   * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
   * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k. Hash-partitions the resulting
   * RDD using the existing partitioner/parallelism level.
    * 执行“this”和“other”的右外连接。 对于“other”中的每个元素（k，w）
    *得到的RDD将包含“this”中的v的所有对（k，（Some（v），w）），或
    * pair（k，（None，w））如果`this`中的元素没有键k。 哈希分割结果
    * RDD使用现有的分区/并行级别。
   */
  def rightOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (Option[V], W))] = self.withScope {
    rightOuterJoin(other, defaultPartitioner(self, other))
  }

  /**
   * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
   * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k. Hash-partitions the resulting
   * RDD into the given number of partitions.
    * 执行“this”和“other”的右外连接。 对于“other”中的每个元素（k，w）
    *得到的RDD将包含“this”中的v的所有对（k，（Some（v），w）），或
    * pair（k，（None，w））如果`this`中的元素没有键k。 哈希分割结果
    * RDD到给定数量的分区。
   */
  def rightOuterJoin[W](
      other: RDD[(K, W)],
      numPartitions: Int): RDD[(K, (Option[V], W))] = self.withScope {
    rightOuterJoin(other, new HashPartitioner(numPartitions))
  }

  /**
   * Perform a full outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (Some(v), Some(w))) for w in `other`, or
   * the pair (k, (Some(v), None)) if no elements in `other` have key k. Similarly, for each
   * element (k, w) in `other`, the resulting RDD will either contain all pairs
   * (k, (Some(v), Some(w))) for v in `this`, or the pair (k, (None, Some(w))) if no elements
   * in `this` have key k. Hash-partitions the resulting RDD using the existing partitioner/
   * parallelism level.
    * 执行“this”和“other”的完整外连接。 对于“this”中的每个元素（k，v）
    *得到的RDD将包含“other”中的w的所有对（k，（Some（v），Some（w））），或
    *对（k，（Some（v），None）））如果`other`中的元素没有键k。 同样，对于每一个
    *元素（k，w）在`other`中，所得到的RDD将包含所有对
    对于“this”中的v，或者对（k，（None，Some（w）））的*（k，（Some（v），Some（w）））
    *在`这个`有关键k。 哈希使用现有的分区器/
    *并行级别。
   */
  def fullOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (Option[V], Option[W]))] = self.withScope {
    fullOuterJoin(other, defaultPartitioner(self, other))
  }

  /**
   * Perform a full outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (Some(v), Some(w))) for w in `other`, or
   * the pair (k, (Some(v), None)) if no elements in `other` have key k. Similarly, for each
   * element (k, w) in `other`, the resulting RDD will either contain all pairs
   * (k, (Some(v), Some(w))) for v in `this`, or the pair (k, (None, Some(w))) if no elements
   * in `this` have key k. Hash-partitions the resulting RDD into the given number of partitions.
   */
  def fullOuterJoin[W](
      other: RDD[(K, W)],
      numPartitions: Int): RDD[(K, (Option[V], Option[W]))] = self.withScope {
    fullOuterJoin(other, new HashPartitioner(numPartitions))
  }

  /**
   * Return the key-value pairs in this RDD to the master as a Map.
   * 返回Master键-值对的Map
   * Warning: this doesn't return a multimap (so if you have multiple values to the same key, only
   *          one value per key is preserved in the map returned)
   */
  def collectAsMap(): Map[K, V] = self.withScope {
    val data = self.collect()
    val map = new mutable.HashMap[K, V]//可变HashMap
    map.sizeHint(data.length)//设置大小
    data.foreach { pair => map.put(pair._1, pair._2) }
    map
  }

  /**
   * Pass each value in the key-value pair RDD through a map function without changing the keys;
   * this also retains the original RDD's partitioning.
   * 将输入的二元tuple数据的value值逐一转换处理,并输出包含原有key和处理后value的二元tuple,最终输出处理后二元tuple的RDD
   * 例如:
   * scala> var rdd1 = sc.makeRDD(Array((1,"A"),(2,"B"),(3,"C"),(4,"D")),2)
   * scala> rdd1.mapValues(x => x + "_").collect
   * res26: Array[(Int, String)] = Array((1,A_), (2,B_), (3,C_), (4,D_))
   */
  def mapValues[U](f: V => U): RDD[(K, U)] = self.withScope {
    val cleanF = self.context.clean(f)
    new MapPartitionsRDD[(K, U), (K, V)](self,
      (context, pid, iter) => iter.map {
  case (k, v) => (k, cleanF(v)) },
preservesPartitioning = true)
}
  /**
   * Pass each value in the key-value pair RDD through a flatMap function without changing the
   * keys; this also retains the original RDD's partitioning.
   * 同基本转换操作中的flatMap,只不过flatMapValues是针对[K,V]中的V值进行flatMap操作
   * 例如:
   * scala> var rdd1 = sc.makeRDD(Array((1,"A"),(2,"B"),(3,"C"),(4,"D")),2)
   * scala> rdd1.flatMapValues(x => x + "_").collect
   * res26: Array[(Int, Char)] = Array((1,A), (1,_), (2,B), (2,_), (3,C), (3,_), (4,D), (4,_))
   */
  def flatMapValues[U](f: V => TraversableOnce[U]): RDD[(K, U)] = self.withScope {
    val cleanF = self.context.clean(f)
    new MapPartitionsRDD[(K, U), (K, V)](self,
      (context, pid, iter) => iter.flatMap { case (k, v) =>
        cleanF(v).map(x => (k, x))
      },
      preservesPartitioning = true)
  }

  /**
   * For each key k in `this` or `other1` or `other2` or `other3`,
   * return a resulting RDD that contains a tuple with the list of values
   * for that key in `this`, `other1`, `other2` and `other3`.
   * 在类型为（K,V)和（K,W)的数据集上调用,返回一个 (K, Seq[V], Seq[W])元组的数据集
   * cogroup相当于SQL中的全外关联full outer join,返回左右RDD中的记录,关联不上的为空。
   * 参数partitioner用于指定分区函数
   */
  def cogroup[W1, W2, W3](other1: RDD[(K, W1)],
      other2: RDD[(K, W2)],
      other3: RDD[(K, W3)],
      partitioner: Partitioner)
      : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] = self.withScope {
    if (partitioner.isInstanceOf[HashPartitioner] && keyClass.isArray) {
      throw new SparkException("Default partitioner cannot partition array keys.")
    }
    val cg = new CoGroupedRDD[K](Seq(self, other1, other2, other3), partitioner)
    cg.mapValues { case Array(vs, w1s, w2s, w3s) =>
       (vs.asInstanceOf[Iterable[V]],
         w1s.asInstanceOf[Iterable[W1]],
         w2s.asInstanceOf[Iterable[W2]],
         w3s.asInstanceOf[Iterable[W3]])
    }
  }

  /**
   * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
   * list of values for that key in `this` as well as `other`.
    * 对于“this”或“other”中的每个键k，返回包含一个元组的结果RDD这个键的值列表'``以及`other`。
   */
  def cogroup[W](other: RDD[(K, W)], partitioner: Partitioner)
      : RDD[(K, (Iterable[V], Iterable[W]))] = self.withScope {
    if (partitioner.isInstanceOf[HashPartitioner] && keyClass.isArray) {
      throw new SparkException("Default partitioner cannot partition array keys.")
    }
    val cg = new CoGroupedRDD[K](Seq(self, other), partitioner)
    cg.mapValues { case Array(vs, w1s) =>
      (vs.asInstanceOf[Iterable[V]], w1s.asInstanceOf[Iterable[W]])
    }
  }

  /**
   * For each key k in `this` or `other1` or `other2`, return a resulting RDD that contains a
   * tuple with the list of values for that key in `this`, `other1` and `other2`.
    * 对于“this”或“other1”或“other2”中的每个键k，返回包含a的结果RDD元组中的这个键的值列表在这个，other1和other2中。
   */
  def cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)], partitioner: Partitioner)
      : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] = self.withScope {
    if (partitioner.isInstanceOf[HashPartitioner] && keyClass.isArray) {
      throw new SparkException("Default partitioner cannot partition array keys.")
    }
    val cg = new CoGroupedRDD[K](Seq(self, other1, other2), partitioner)
    cg.mapValues { case Array(vs, w1s, w2s) =>
      (vs.asInstanceOf[Iterable[V]],
        w1s.asInstanceOf[Iterable[W1]],
        w2s.asInstanceOf[Iterable[W2]])
    }
  }

  /**
   * For each key k in `this` or `other1` or `other2` or `other3`,
   * return a resulting RDD that contains a tuple with the list of values
    * 对于“this”或“other1”或“other2”或“other3”中的每个键k，返回包含元组列表的结果RDD
   * 返回一个包含RDD值列表元组
   * for that key in `this`, `other1`, `other2` and `other3`.
    * 对于“this”，“other1”，“other2”和“other3”中的该键。
   */
  def cogroup[W1, W2, W3](other1: RDD[(K, W1)], other2: RDD[(K, W2)], other3: RDD[(K, W3)])
      : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] = self.withScope {
    cogroup(other1, other2, other3, defaultPartitioner(self, other1, other2, other3))
  }

  /**
   * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
   * list of values for that key in `this` as well as `other`.
    * 对于`this`或`other`中的每个键k，返回一个包含一个元组的结果RDD，其中该键的值列表为`this`以及`other`
   */
  def cogroup[W](other: RDD[(K, W)]): RDD[(K, (Iterable[V], Iterable[W]))] = self.withScope {
    cogroup(other, defaultPartitioner(self, other))
  }

  /**
   * For each key k in `this` or `other1` or `other2`, return a resulting RDD that contains a
   * tuple with the list of values for that key in `this`, `other1` and `other2`.
    * 对于“this”或“other1”或“other2”中的每个键k，返回包含a的结果RDD元组中的这个键的值列表在这个，other1和other2中。
   */
  def cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)])
      : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] = self.withScope {
    cogroup(other1, other2, defaultPartitioner(self, other1, other2))
  }

  /**
   * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
   * list of values for that key in `this` as well as `other`.
    * 对于“this”或“other”中的每个键k，返回包含一个元组的结果RDD该键的值列表以及“其他”。
   */
  def cogroup[W](
      other: RDD[(K, W)],
      numPartitions: Int): RDD[(K, (Iterable[V], Iterable[W]))] = self.withScope {
    cogroup(other, new HashPartitioner(numPartitions))
  }

  /**
   * For each key k in `this` or `other1` or `other2`, return a resulting RDD that contains a
   * tuple with the list of values for that key in `this`, `other1` and `other2`.
    * 对于“this”或“other1”或“other2”中的每个键k，返回包含a的结果RDD元组中的这个键的值列表在这个，other1和other2中。
   */
  def cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)], numPartitions: Int)
      : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] = self.withScope {
    cogroup(other1, other2, new HashPartitioner(numPartitions))
  }

  /**
   * For each key k in `this` or `other1` or `other2` or `other3`,
   * return a resulting RDD that contains a tuple with the list of values
   * for that key in `this`, `other1`, `other2` and `other3`.
    *
    * 对于“this”或“other1”或“other2”或“other3”中的每个键k，返回包含元组的结果RDD,
    * 该元组的名称为“this”，“other1”，“other2”和other3
   */
  def cogroup[W1, W2, W3](other1: RDD[(K, W1)],
      other2: RDD[(K, W2)],
      other3: RDD[(K, W3)],
      numPartitions: Int)
      : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] = self.withScope {
    cogroup(other1, other2, other3, new HashPartitioner(numPartitions))
  }

  /** Alias for cogroup. 别名为cogroup*/
  def groupWith[W](other: RDD[(K, W)]): RDD[(K, (Iterable[V], Iterable[W]))] = self.withScope {
    cogroup(other, defaultPartitioner(self, other))
  }

  /** Alias for cogroup. 别名为cogroup*/
  def groupWith[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)])
      : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] = self.withScope {
    cogroup(other1, other2, defaultPartitioner(self, other1, other2))
  }

  /** Alias for cogroup. 别名为cogroup*/
  def groupWith[W1, W2, W3](other1: RDD[(K, W1)], other2: RDD[(K, W2)], other3: RDD[(K, W3)])
      : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] = self.withScope {
    cogroup(other1, other2, other3, defaultPartitioner(self, other1, other2, other3))
  }

  /**
   * Return an RDD with the pairs from `this` whose keys are not in `other`.
   * 返回在RDD中出现,并且不在other RDD中出现的元素(交集,相当于进行集合的差操作),不去重,
   * 只不过这里是针对K的,返回在主RDD中出现,并且不在otherRDD中出现的元素。
   * Uses `this` partitioner/partition size, because even if `other` is huge, the resulting
   * RDD will be <= us.
   * 例如:
   * var rdd1 = sc.makeRDD(Array(("A","1"),("B","2"),("C","3")),2)
   * var rdd2 = sc.makeRDD(Array(("A","a"),("C","c"),("D","d")),2)
   * scala> rdd1.subtractByKey(rdd2).collect
   * res13: Array[(String, String)] = Array((B,2))
   */
  def subtractByKey[W: ClassTag](other: RDD[(K, W)]): RDD[(K, V)] = self.withScope {
    subtractByKey(other, self.partitioner.getOrElse(new HashPartitioner(self.partitions.length)))
  }

  /** 
   *  Return an RDD with the pairs from `this` whose keys are not in `other`. 
   *  返回在RDD中出现,并且不在other RDD中出现的元素(交集,相当于进行集合的差操作),不去重
   *  */
  def subtractByKey[W: ClassTag](
      other: RDD[(K, W)],
      numPartitions: Int): RDD[(K, V)] = self.withScope {
    subtractByKey(other, new HashPartitioner(numPartitions))
  }

  /** 
   *  Return an RDD with the pairs from `this` whose keys are not in `other`. 
   *  返回在RDD中出现,并且不在other RDD中出现的元素(交集,相当于进行集合的差操作),不去重
   *  */
  def subtractByKey[W: ClassTag](other: RDD[(K, W)], p: Partitioner): RDD[(K, V)] = self.withScope {
    new SubtractedRDD[K, V, W](self, other, p)
  }

  /**
   * Return the list of values in the RDD for key `key`. This operation is done efficiently if the
   * RDD has a known partitioner by only searching the partition that the key maps to.
   * lookup用于(K,V)类型的RDD,指定K值,返回RDD中该K对应的所有V值。
   * 遍历RDD中所有的key,找到符合输入key的value,并输出scala的seq数据
   * 例如:
   * scala> var rdd1 = sc.makeRDD(Array(("A",0),("A",2),("B",1),("B",2),("C",1)))
   * scala> rdd1.lookup("A")
   * res0: Seq[Int] = WrappedArray(0, 2)
   * scala> rdd1.lookup("B")
   * res1: Seq[Int] = WrappedArray(1, 2)
   */
  def lookup(key: K): Seq[V] = self.withScope {
    self.partitioner match {
      case Some(p) =>
        val index = p.getPartition(key)
        val process = (it: Iterator[(K, V)]) => {
          val buf = new ArrayBuffer[V]
          for (pair <- it if pair._1 == key) {
            buf += pair._2
          }
          buf
        } : Seq[V]
        val res = self.context.runJob(self, process, Array(index))
        res(0)
      case None =>
        self.filter(_._1 == key).map(_._2).collect()
    }
  }

  /**
   * Output the RDD to any Hadoop-supported file system, using a Hadoop `OutputFormat` class
   * supporting the key and value types K and V in this RDD.
    * 使用支持该RDD中的键和值类型K和V的Hadoop`OutputFormat`类将RDD输出到任何Hadoop支持的文件系统。
   */
  def saveAsHadoopFile[F <: OutputFormat[K, V]](
      path: String)(implicit fm: ClassTag[F]): Unit = self.withScope {
    saveAsHadoopFile(path, keyClass, valueClass, fm.runtimeClass.asInstanceOf[Class[F]])
  }

  /**
   * Output the RDD to any Hadoop-supported file system, using a Hadoop `OutputFormat` class
   * supporting the key and value types K and V in this RDD. Compress the result with the
   * supplied codec.
    * 使用支持该RDD中的键和值类型K和V的Hadoop`OutputFormat`类将RDD输出到任何Hadoop支持的文件系统,使用提供的编解码器压缩结果
   */
  def saveAsHadoopFile[F <: OutputFormat[K, V]](
      path: String,
      codec: Class[_ <: CompressionCodec])(implicit fm: ClassTag[F]): Unit = self.withScope {
    val runtimeClass = fm.runtimeClass
    saveAsHadoopFile(path, keyClass, valueClass, runtimeClass.asInstanceOf[Class[F]], codec)
  }

  /**
   * Output the RDD to any Hadoop-supported file system, using a new Hadoop API `OutputFormat`
   * (mapreduce.OutputFormat) object supporting the key and value types K and V in this RDD.
    * 使用新的Hadoop API“OutputFormat”(mapreduce.OutputFormat)对象支持该RDD中的键和值类型K和V,将RDD输出到任何Hadoop支持的文件系统。
   */
  def saveAsNewAPIHadoopFile[F <: NewOutputFormat[K, V]](
      path: String)(implicit fm: ClassTag[F]): Unit = self.withScope {
    saveAsNewAPIHadoopFile(path, keyClass, valueClass, fm.runtimeClass.asInstanceOf[Class[F]])
  }

  /**
   * Output the RDD to any Hadoop-supported file system, using a new Hadoop API `OutputFormat`
   * (mapreduce.OutputFormat) object supporting the key and value types K and V in this RDD.
    * 使用新的Hadoop API“OutputFormat”（mapreduce.OutputFormat）
    * 对象支持该RDD中的键和值类型K和V，将RDD输出到任何Hadoop支持的文件系统。
   */
  def saveAsNewAPIHadoopFile(
      path: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[_ <: NewOutputFormat[_, _]],
      conf: Configuration = self.context.hadoopConfiguration): Unit = self.withScope {
    // Rename this as hadoopConf internally to avoid shadowing (see SPARK-2038).
    //将其重命名为hadoopConf以避免阴影（参见SPARK-2038）。
    val hadoopConf = conf
    val job = new NewAPIHadoopJob(hadoopConf)
    job.setOutputKeyClass(keyClass)
    job.setOutputValueClass(valueClass)
    job.setOutputFormatClass(outputFormatClass)
    job.getConfiguration.set("mapred.output.dir", path)
    saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  /**
   * Output the RDD to any Hadoop-supported file system, using a Hadoop `OutputFormat` class
   * supporting the key and value types K and V in this RDD. Compress with the supplied codec.
    * 使用支持该RDD中的键和值类型K和V的Hadoop`OutputFormat`类将RDD输出到任何Hadoop支持的文件系统。 使用提供的编解码器进行压缩
   */
  def saveAsHadoopFile(
      path: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[_ <: OutputFormat[_, _]],
      codec: Class[_ <: CompressionCodec]): Unit = self.withScope {
    saveAsHadoopFile(path, keyClass, valueClass, outputFormatClass,
      new JobConf(self.context.hadoopConfiguration), Some(codec))
  }

  /**
   * Output the RDD to any Hadoop-supported file system, using a Hadoop `OutputFormat` class
   * supporting the key and value types K and V in this RDD.
    * 使用Hadoop`OutputFormat`类将RDD输出到任何支持Hadoop的文件系统支持此RDD中的键和值类型K和V.
   */
  def saveAsHadoopFile(
      path: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[_ <: OutputFormat[_, _]],
      conf: JobConf = new JobConf(self.context.hadoopConfiguration),
      codec: Option[Class[_ <: CompressionCodec]] = None): Unit = self.withScope {
    // Rename this as hadoopConf internally to avoid shadowing (see SPARK-2038).
    //将其重命名为hadoopConf以避免阴影（参见SPARK-2038）。
    val hadoopConf = conf
    hadoopConf.setOutputKeyClass(keyClass)
    hadoopConf.setOutputValueClass(valueClass)
    // Doesn't work in Scala 2.9 due to what may be a generics bug
    //在Scala 2.9中不起作用，因为可能是泛型错误
    // TODO: Should we uncomment this for Scala 2.10?
    // conf.setOutputFormat(outputFormatClass)
    hadoopConf.set("mapred.output.format.class", outputFormatClass.getName)
    for (c <- codec) {
      hadoopConf.setCompressMapOutput(true)
      hadoopConf.set("mapred.output.compress", "true")
      hadoopConf.setMapOutputCompressorClass(c)
      hadoopConf.set("mapred.output.compression.codec", c.getCanonicalName)
      hadoopConf.set("mapred.output.compression.type", CompressionType.BLOCK.toString)
    }

    // Use configured output committer if already set
    //如果已设置,请使用配置的输出提交者
    if (conf.getOutputCommitter == null) {
      hadoopConf.setOutputCommitter(classOf[FileOutputCommitter])
    }

    FileOutputFormat.setOutputPath(hadoopConf,
      SparkHadoopWriter.createPathFromString(path, hadoopConf))
    saveAsHadoopDataset(hadoopConf)
  }

  /**
   * Output the RDD to any Hadoop-supported storage system with new Hadoop API, using a Hadoop
   * Configuration object for that storage system. The Conf should set an OutputFormat and any
   * output paths required (e.g. a table name to write to) in the same way as it would be
   * configured for a Hadoop MapReduce job.
    * 使用Hadoop将RDD输出到具有新Hadoop API的任何Hadoop支持的存储系统
    *该存储系统的配置对象。 Conf应该设置一个OutputFormat和任何
    *所需的输出路径（例如要写入的表名称）的方式与之相同
    *配置为Hadoop MapReduce作业。
   */
  def saveAsNewAPIHadoopDataset(conf: Configuration): Unit = self.withScope {
    // Rename this as hadoopConf internally to avoid shadowing (see SPARK-2038).
    //将其重命名为hadoopConf以避免阴影（参见SPARK-2038）。
    val hadoopConf = conf
    val job = new NewAPIHadoopJob(hadoopConf)
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    val jobtrackerID = formatter.format(new Date())
    val stageId = self.id
    val wrappedConf = new SerializableConfiguration(job.getConfiguration)
    val outfmt = job.getOutputFormatClass
    val jobFormat = outfmt.newInstance

    if (isOutputSpecValidationEnabled) {
      // FileOutputFormat ignores the filesystem parameter
      //FileOutputFormat忽略文件系统参数
      jobFormat.checkOutputSpecs(job)
    }

    val writeShard = (context: TaskContext, iter: Iterator[(K, V)]) => {
      val config = wrappedConf.value
      /* "reduce task" <split #> <attempt # = spark task #> */
      val attemptId = newTaskAttemptID(jobtrackerID, stageId, isMap = false, context.partitionId,
        context.attemptNumber)
      val hadoopContext = newTaskAttemptContext(config, attemptId)
      val format = outfmt.newInstance
      format match {
        case c: Configurable => c.setConf(config)
        case _ => ()
      }
      val committer = format.getOutputCommitter(hadoopContext)
      committer.setupTask(hadoopContext)

      val (outputMetrics, bytesWrittenCallback) = initHadoopOutputMetrics(context)

      val writer = format.getRecordWriter(hadoopContext).asInstanceOf[NewRecordWriter[K, V]]
      require(writer != null, "Unable to obtain RecordWriter")
      var recordsWritten = 0L
      Utils.tryWithSafeFinally {
        while (iter.hasNext) {
          val pair = iter.next()
          writer.write(pair._1, pair._2)

          // Update bytes written metric every few records
          //每隔几个记录更新写入的字节数
          maybeUpdateOutputMetrics(bytesWrittenCallback, outputMetrics, recordsWritten)
          recordsWritten += 1
        }
      } {
        writer.close(hadoopContext)
      }
      committer.commitTask(hadoopContext)
      bytesWrittenCallback.foreach { fn => outputMetrics.setBytesWritten(fn()) }
      outputMetrics.setRecordsWritten(recordsWritten)
      1
    } : Int

    val jobAttemptId = newTaskAttemptID(jobtrackerID, stageId, isMap = true, 0, 0)
    val jobTaskContext = newTaskAttemptContext(wrappedConf.value, jobAttemptId)
    val jobCommitter = jobFormat.getOutputCommitter(jobTaskContext)
    jobCommitter.setupJob(jobTaskContext)
    self.context.runJob(self, writeShard)
    jobCommitter.commitJob(jobTaskContext)
  }

  /**
   * Output the RDD to any Hadoop-supported storage system, using a Hadoop JobConf object for
   * that storage system. The JobConf should set an OutputFormat and any output paths required
   * (e.g. a table name to write to) in the same way as it would be configured for a Hadoop
   * MapReduce job.
    *
    * 使用Hadoop JobConf对象将RDD输出到任何Hadoop支持的存储系统那个存储系统。
    * JobConf应设置一个OutputFormat和任何需要的输出路径（例如要写入的表名）以与为Hadoop配置的相同的方式
    * MapReduce工作。输出RDD任何Hadoopf支持的存储系统,使用Hadoop JobConf对象存储系统.
   */
  def saveAsHadoopDataset(conf: JobConf): Unit = self.withScope {
    // Rename this as hadoopConf internally to avoid shadowing (see SPARK-2038).
    //将其重命名为hadoopConf以避免阴影（参见SPARK-2038）。
    val hadoopConf = conf
    val wrappedConf = new SerializableConfiguration(hadoopConf)
    val outputFormatInstance = hadoopConf.getOutputFormat
    val keyClass = hadoopConf.getOutputKeyClass
    val valueClass = hadoopConf.getOutputValueClass
    if (outputFormatInstance == null) {
      throw new SparkException("Output format class not set")
    }
    if (keyClass == null) {
      throw new SparkException("Output key class not set")
    }
    if (valueClass == null) {
      throw new SparkException("Output value class not set")
    }
    SparkHadoopUtil.get.addCredentials(hadoopConf)

    logDebug("Saving as hadoop file of type (" + keyClass.getSimpleName + ", " +
      valueClass.getSimpleName + ")")

    if (isOutputSpecValidationEnabled) {
      // FileOutputFormat ignores the filesystem parameter
      //FileOutputFormat忽略文件系统参数
      val ignoredFs = FileSystem.get(hadoopConf)
      hadoopConf.getOutputFormat.checkOutputSpecs(ignoredFs, hadoopConf)
    }

    val writer = new SparkHadoopWriter(hadoopConf)
    writer.preSetup()

    val writeToFile = (context: TaskContext, iter: Iterator[(K, V)]) => {
      val config = wrappedConf.value
      // Hadoop wants a 32-bit task attempt ID, so if ours is bigger than Int.MaxValue, roll it
      // around by taking a mod. We expect that no task will be attempted 2 billion times.
      //Hadoop想要一个32位的任务尝试ID,所以如果我们的大于Int.MaxValue,通过取一个mod来滚动它,我们期望没有任何任务将尝试20亿次。
      val taskAttemptId = (context.taskAttemptId % Int.MaxValue).toInt

      val (outputMetrics, bytesWrittenCallback) = initHadoopOutputMetrics(context)

      writer.setup(context.stageId, context.partitionId, taskAttemptId)
      writer.open()
      var recordsWritten = 0L

      Utils.tryWithSafeFinally {
        while (iter.hasNext) {
          val record = iter.next()
          writer.write(record._1.asInstanceOf[AnyRef], record._2.asInstanceOf[AnyRef])

          // Update bytes written metric every few records
          //每隔几个记录更新写入的字节数
          maybeUpdateOutputMetrics(bytesWrittenCallback, outputMetrics, recordsWritten)
          recordsWritten += 1
        }
      } {
        writer.close()
      }
      writer.commit()
      bytesWrittenCallback.foreach { fn => outputMetrics.setBytesWritten(fn()) }
      outputMetrics.setRecordsWritten(recordsWritten)
    }

    self.context.runJob(self, writeToFile)
    writer.commitJob()
  }

  private def initHadoopOutputMetrics(context: TaskContext): (OutputMetrics, Option[() => Long]) = {
    val bytesWrittenCallback = SparkHadoopUtil.get.getFSBytesWrittenOnThreadCallback()
    val outputMetrics = new OutputMetrics(DataWriteMethod.Hadoop)
    if (bytesWrittenCallback.isDefined) {
      context.taskMetrics.outputMetrics = Some(outputMetrics)
    }
    (outputMetrics, bytesWrittenCallback)
  }

  private def maybeUpdateOutputMetrics(bytesWrittenCallback: Option[() => Long],
      outputMetrics: OutputMetrics, recordsWritten: Long): Unit = {
    if (recordsWritten % PairRDDFunctions.RECORDS_BETWEEN_BYTES_WRITTEN_METRIC_UPDATES == 0) {
      bytesWrittenCallback.foreach { fn => outputMetrics.setBytesWritten(fn()) }
      outputMetrics.setRecordsWritten(recordsWritten)
    }
  }

  /**
   * Return an RDD with the keys of each tuple.
   * 从RDD元组中抽取所有元素的key并生成新的RDD
   */
  def keys: RDD[K] = self.map(_._1)

  /**
   * Return an RDD with the values of each tuple.
   * 从RDD元组中抽取所有元素的value并生成新的RD
   */
  def values: RDD[V] = self.map(_._2)
  //keyClass是数组时需要特殊的partitioner,默认的HashPartitioner不支持数组 
  private[spark] def keyClass: Class[_] = kt.runtimeClass

  private[spark] def valueClass: Class[_] = vt.runtimeClass

  private[spark] def keyOrdering: Option[Ordering[K]] = Option(ord)

  // Note: this needs to be a function instead of a 'val' so that the disableOutputSpecValidation
  // setting can take effect:
  //注意：这需要一个函数而不是'val'，以便disableOutputSpecValidation设置可以生效
  private def isOutputSpecValidationEnabled: Boolean = {
    val validationDisabled = PairRDDFunctions.disableOutputSpecValidation.value
    val enabledInConf = self.conf.getBoolean("spark.hadoop.validateOutputSpecs", true)
    enabledInConf && !validationDisabled
  }
}

private[spark] object PairRDDFunctions {
  val RECORDS_BETWEEN_BYTES_WRITTEN_METRIC_UPDATES = 256

  /**
   * Allows for the `spark.hadoop.validateOutputSpecs` checks to be disabled on a case-by-case
   * basis; see SPARK-4835 for more details.
    * 允许根据具体情况禁用“spark.hadoop.validateOutputSpecs”检查; 有关详细信息，请参阅SPARK-4835。
   */
  val disableOutputSpecValidation: DynamicVariable[Boolean] = new DynamicVariable[Boolean](false)
}
