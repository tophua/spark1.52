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

package org.apache.spark.sql.execution

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.hash.HashShuffleManager
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.shuffle.unsafe.UnsafeShuffleManager
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors.attachTree
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.util.MutablePair
import org.apache.spark.{HashPartitioner, Partitioner, RangePartitioner, SparkEnv}

/**
 * :: DeveloperApi ::
 * Performs a shuffle that will result in the desired `newPartitioning`.
  *执行一个将导致所需`newPartitioning`的shuffle
 */
@DeveloperApi
case class Exchange(newPartitioning: Partitioning, child: SparkPlan) extends UnaryNode {

  override def nodeName: String = if (tungstenMode) "TungstenExchange" else "Exchange"

  /**
   * Returns true iff we can support the data type, and we are not doing range partitioning.
    * 如果我们可以支持数据类型,则返回true,并且我们不进行范围分区
   */
  private lazy val tungstenMode: Boolean = {
    unsafeEnabled && codegenEnabled && GenerateUnsafeProjection.canSupport(child.schema) &&
      !newPartitioning.isInstanceOf[RangePartitioning]
  }

  override def outputPartitioning: Partitioning = newPartitioning

  override def output: Seq[Attribute] = child.output

  // This setting is somewhat counterintuitive:
  // If the schema works with UnsafeRow, then we tell the planner that we don't support safe row,
  // so the planner inserts a converter to convert data into UnsafeRow if needed.
  //如果架构与UnsafeRow一起使用,那么我们告诉规划器我们不支持安全行,
  //因此如果需要,规划器会插入转换器以将数据转换为UnsafeRow。
  override def outputsUnsafeRows: Boolean = tungstenMode
  override def canProcessSafeRows: Boolean = !tungstenMode
  override def canProcessUnsafeRows: Boolean = tungstenMode

  /**
   * Determines whether records must be defensively copied before being sent to the shuffle.
    * 确定在发送到shuffle之前是否必须防御性地复制记录
   * Several of Spark's shuffle components will buffer deserialized Java objects in memory. The
   * shuffle code assumes that objects are immutable and hence does not perform its own defensive
   * copying. In Spark SQL, however, operators' iterators return the same mutable `Row` object. In
   * order to properly shuffle the output of these operators, we need to perform our own copying
   * prior to sending records to the shuffle. This copying is expensive, so we try to avoid it
   * whenever possible. This method encapsulates the logic for choosing when to copy.
    *
    * Spark的几个shuffle组件将缓冲内存中反序列化的Java对象,随机代码假定对象是不可变的,
    * 因此不执行自己的防御性复制,但是,在Spark SQL中,运算符的迭代器返回相同的可变`Row`对象。
    * 为了正确地改变这些运算符的输出,我们需要在将记录发送到shuffle之前执行我们自己的复制,
    * 这种复制很昂贵,所以尽可能避免使用它,此方法封装了选择何时复制的逻辑。
   *
   * In the long run, we might want to push this logic into core's shuffle APIs so that we don't
   * have to rely on knowledge of core internals here in SQL.
    * 从长远来看,我们可能希望将此逻辑推入核心的shuffle API中,这样我们就不必依赖SQL中核心内部的知识。
   *
   * See SPARK-2967, SPARK-4479, and SPARK-7375 for more discussion of this issue.
   *
   * @param partitioner the partitioner for the shuffle shuffle的分区器
   * @param serializer the serializer that will be used to write rows 将用于写行的序列化程序
   * @return true if rows should be copied before being shuffled, false otherwise
    *         如果要在洗牌之前复制行,则返回true,否则返回false
   */
  private def needToCopyObjectsBeforeShuffle(
      partitioner: Partitioner,
      serializer: Serializer): Boolean = {
    // Note: even though we only use the partitioner's `numPartitions` field, we require it to be
    // passed instead of directly passing the number of partitions in order to guard against
    // corner-cases where a partitioner constructed with `numPartitions` partitions may output
    // fewer partitions (like RangePartitioner, for example).
    val conf = child.sqlContext.sparkContext.conf
    val shuffleManager = SparkEnv.get.shuffleManager
    val sortBasedShuffleOn = shuffleManager.isInstanceOf[SortShuffleManager] ||
      shuffleManager.isInstanceOf[UnsafeShuffleManager]
    //如果partition数目少于bypassMergeThreshold的值,不需要在Executor执行聚合和排序操作,直接将每个partition写入单独的文件,最后在reduce端再做串联
    val bypassMergeThreshold = conf.getInt("spark.shuffle.sort.bypassMergeThreshold", 200)
    val serializeMapOutputs = conf.getBoolean("spark.shuffle.sort.serializeMapOutputs", true)
    if (sortBasedShuffleOn) {
      val bypassIsSupported = SparkEnv.get.shuffleManager.isInstanceOf[SortShuffleManager]
      if (bypassIsSupported && partitioner.numPartitions <= bypassMergeThreshold) {
        // If we're using the original SortShuffleManager and the number of output partitions is
        // sufficiently small, then Spark will fall back to the hash-based shuffle write path, which
        // doesn't buffer deserialized records.
        // Note that we'll have to remove this case if we fix SPARK-6026 and remove this bypass.
        false
      } else if (serializeMapOutputs && serializer.supportsRelocationOfSerializedObjects) {
        // SPARK-4550 extended sort-based shuffle to serialize individual records prior to sorting
        // them. This optimization is guarded by a feature-flag and is only applied in cases where
        // shuffle dependency does not specify an aggregator or ordering and the record serializer
        // has certain properties. If this optimization is enabled, we can safely avoid the copy.
        //
        // Exchange never configures its ShuffledRDDs with aggregators or key orderings, so we only
        // need to check whether the optimization is enabled and supported by our serializer.
        //
        // This optimization also applies to UnsafeShuffleManager (added in SPARK-7081).
        false
      } else {
        // Spark's SortShuffleManager uses `ExternalSorter` to buffer records in memory. This code
        // path is used both when SortShuffleManager is used and when UnsafeShuffleManager falls
        // back to SortShuffleManager to perform a shuffle that the new fast path can't handle. In
        // both cases, we must copy.
        true
      }
    } else if (shuffleManager.isInstanceOf[HashShuffleManager]) {
      // We're using hash-based shuffle, so we don't need to copy.
      false
    } else {
      // Catch-all case to safely handle any future ShuffleManager implementations.
      true
    }
  }

  @transient private lazy val sparkConf = child.sqlContext.sparkContext.getConf

  private val serializer: Serializer = {
    val rowDataTypes = child.output.map(_.dataType).toArray
    if (tungstenMode) {
      new UnsafeRowSerializer(child.output.size)
    } else {
      new SparkSqlSerializer(sparkConf)
    }
  }

  protected override def doExecute(): RDD[InternalRow] = attachTree(this , "execute") {
    val rdd = child.execute()
    val part: Partitioner = newPartitioning match {
      case HashPartitioning(expressions, numPartitions) => new HashPartitioner(numPartitions)
      case RangePartitioning(sortingExpressions, numPartitions) =>
        // Internally, RangePartitioner runs a job on the RDD that samples keys to compute
        // partition bounds. To get accurate samples, we need to copy the mutable keys.
        //在内部，RangePartitioner在RDD上运行一个作业,该作业对密钥进行采样以计算分区边界,为了获得准确的样本,我们需要复制可变密钥
        val rddForSampling = rdd.mapPartitions { iter =>
          val mutablePair = new MutablePair[InternalRow, Null]()
          iter.map(row => mutablePair.update(row.copy(), null))
        }
        // We need to use an interpreted ordering here because generated orderings cannot be
        // serialized and this ordering needs to be created on the driver in order to be passed into
        // Spark core code.
        //我们需要在这里使用解释的排序,因为生成的排序无法序列化,并且需要在驱动程序上创建此排序以便传递给Spark核心代码
        implicit val ordering = new InterpretedOrdering(sortingExpressions, child.output)
        new RangePartitioner(numPartitions, rddForSampling, ascending = true)
      case SinglePartition =>
        new Partitioner {
          override def numPartitions: Int = 1
          override def getPartition(key: Any): Int = 0
        }
      case _ => sys.error(s"Exchange not implemented for $newPartitioning")
      // TODO: Handle BroadcastPartitioning.
    }
    def getPartitionKeyExtractor(): InternalRow => InternalRow = newPartitioning match {
      case HashPartitioning(expressions, _) => newMutableProjection(expressions, child.output)()
      case RangePartitioning(_, _) | SinglePartition => identity
      case _ => sys.error(s"Exchange not implemented for $newPartitioning")
    }
    val rddWithPartitionIds: RDD[Product2[Int, InternalRow]] = {
      if (needToCopyObjectsBeforeShuffle(part, serializer)) {
        rdd.mapPartitions { iter =>
          val getPartitionKey = getPartitionKeyExtractor()
          iter.map { row => (part.getPartition(getPartitionKey(row)), row.copy()) }
        }
      } else {
        rdd.mapPartitions { iter =>
          val getPartitionKey = getPartitionKeyExtractor()
          val mutablePair = new MutablePair[Int, InternalRow]()
          iter.map { row => mutablePair.update(part.getPartition(getPartitionKey(row)), row) }
        }
      }
    }
    new ShuffledRowRDD(rddWithPartitionIds, serializer, part.numPartitions)
  }
}

/**
 * Ensures that the [[org.apache.spark.sql.catalyst.plans.physical.Partitioning Partitioning]]
 * of input data meets the
 * [[org.apache.spark.sql.catalyst.plans.physical.Distribution Distribution]] requirements for
 * each operator by inserting [[Exchange]] Operators where required.  Also ensure that the
 * input partition ordering requirements are met.
 */
private[sql] case class EnsureRequirements(sqlContext: SQLContext) extends Rule[SparkPlan] {
  // TODO: Determine the number of partitions.
  private def numPartitions: Int = sqlContext.conf.numShufflePartitions

  /**
   * Given a required distribution, returns a partitioning that satisfies that distribution.
    * 给定必需的分布,返回满足该分布的分区
   */
  private def canonicalPartitioning(requiredDistribution: Distribution): Partitioning = {
    requiredDistribution match {
      case AllTuples => SinglePartition
      case ClusteredDistribution(clustering) => HashPartitioning(clustering, numPartitions)
      case OrderedDistribution(ordering) => RangePartitioning(ordering, numPartitions)
      case dist => sys.error(s"Do not know how to satisfy distribution $dist")
    }
  }

  private def ensureDistributionAndOrdering(operator: SparkPlan): SparkPlan = {
    val requiredChildDistributions: Seq[Distribution] = operator.requiredChildDistribution
    val requiredChildOrderings: Seq[Seq[SortOrder]] = operator.requiredChildOrdering
    var children: Seq[SparkPlan] = operator.children
    assert(requiredChildDistributions.length == children.length)
    assert(requiredChildOrderings.length == children.length)

    // Ensure that the operator's children satisfy their output distribution requirements:
    //确保操作员的孩子满足他们的输出分配要求
    children = children.zip(requiredChildDistributions).map { case (child, distribution) =>
      if (child.outputPartitioning.satisfies(distribution)) {
        child
      } else {
        Exchange(canonicalPartitioning(distribution), child)
      }
    }

    // If the operator has multiple children and specifies child output distributions (e.g. join),
    // then the children's output partitionings must be compatible:
    //如果运算符有多个子节点并指定子输出分布(例如join),则子节点的输出分区必须兼容：
    if (children.length > 1
        && requiredChildDistributions.toSet != Set(UnspecifiedDistribution)
        && !Partitioning.allCompatible(children.map(_.outputPartitioning))) {
      children = children.zip(requiredChildDistributions).map { case (child, distribution) =>
        val targetPartitioning = canonicalPartitioning(distribution)
        if (child.outputPartitioning.guarantees(targetPartitioning)) {
          child
        } else {
          Exchange(targetPartitioning, child)
        }
      }
    }

    // Now that we've performed any necessary shuffles, add sorts to guarantee output orderings:
    //现在我们已经执行了任何必要的shuffle,添加排序以保证输出顺序
    children = children.zip(requiredChildOrderings).map { case (child, requiredOrdering) =>
      if (requiredOrdering.nonEmpty) {
        // If child.outputOrdering is [a, b] and requiredOrdering is [a], we do not need to sort.
        if (requiredOrdering != child.outputOrdering.take(requiredOrdering.length)) {
          sqlContext.planner.BasicOperators.getSortOperator(requiredOrdering, global = false, child)
        } else {
          child
        }
      } else {
        child
      }
    }

    operator.withNewChildren(children)
  }

  def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case operator: SparkPlan => ensureDistributionAndOrdering(operator)
  }
}
