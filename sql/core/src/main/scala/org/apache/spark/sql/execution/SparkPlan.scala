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

import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.{RDD, RDDOperationScope}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.metric.{LongSQLMetric, SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.DataType

object SparkPlan {
  protected[sql] val currentContext = new ThreadLocal[SQLContext]()
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
abstract class SparkPlan extends QueryPlan[SparkPlan] with Logging with Serializable {

  /**
   * A handle to the SQL Context that was used to create this plan.   Since many operators need
   * access to the sqlContext for RDD operations or configuration this field is automatically
   * populated by the query planning infrastructure.
    * 用于创建此计划的SQL上下文的句柄,由于许多运营商需要访问sqlContext以进行RDD操作或配置,
    * 因此查询计划基础结构会自动填充此字段
   */
  @transient
  protected[spark] final val sqlContext = SparkPlan.currentContext.get()

  protected def sparkContext = sqlContext.sparkContext

  // sqlContext will be null when we are being deserialized on the slaves.  In this instance
  // the value of codegenEnabled/unsafeEnabled will be set by the desserializer after the
  // constructor has run.
  //当我们在从属设备上反序列化时,sqlContext将为null,在这种情况下,
  //codegenEnabled / unsafeEnabled的值将由desserializer设置后
  val codegenEnabled: Boolean = if (sqlContext != null) {
    sqlContext.conf.codegenEnabled
  } else {
    false
  }
  val unsafeEnabled: Boolean = if (sqlContext != null) {
    sqlContext.conf.unsafeEnabled
  } else {
    false
  }

  /**
   * Whether the "prepare" method is called.
    * 是否调用“prepare”方法
   */
  private val prepareCalled = new AtomicBoolean(false)

  /** Overridden make copy also propogates sqlContext to copied plan.
    * 重写make copy也会将sqlContext传播给复制的计划*/
  override def makeCopy(newArgs: Array[AnyRef]): SparkPlan = {
    SparkPlan.currentContext.set(sqlContext)
    super.makeCopy(newArgs)
  }

  /**
   * Return all metrics containing metrics of this SparkPlan.
    * 返回包含此SparkPlan指标的所有指标
   */
  private[sql] def metrics: Map[String, SQLMetric[_, _]] = Map.empty

  /**
   * Return a LongSQLMetric according to the name.
    * 根据名称返回LongSQLMetric
   */
  private[sql] def longMetric(name: String): LongSQLMetric =
    metrics(name).asInstanceOf[LongSQLMetric]

  // TODO: Move to `DistributedPlan`
  /** Specifies how data is partitioned across different nodes in the cluster.
    * 指定如何跨群集中的不同节点对数据进行分区*/
  def outputPartitioning: Partitioning = UnknownPartitioning(0) // TODO: WRONG WIDTH!

  /** Specifies any partition requirements on the input data for this operator.
    * 指定此运算符的输入数据的任何分区要求*/
  def requiredChildDistribution: Seq[Distribution] =
    Seq.fill(children.size)(UnspecifiedDistribution)

  /** Specifies how data is ordered in each partition.
    * 指定每个分区中数据的排序方式*/
  def outputOrdering: Seq[SortOrder] = Nil

  /** Specifies sort order for each partition requirements on the input data for this operator.
    * 指定此运算符的输入数据上每个分区要求的排序顺序*/
  def requiredChildOrdering: Seq[Seq[SortOrder]] = Seq.fill(children.size)(Nil)

  /** Specifies whether this operator outputs UnsafeRows
    * 指定此运算符是否输出UnsafeRows*/
  def outputsUnsafeRows: Boolean = false

  /** Specifies whether this operator is capable of processing UnsafeRows
    * 指定此运算符是否能够处理UnsafeRows*/
  def canProcessUnsafeRows: Boolean = false

  /**
   * Specifies whether this operator is capable of processing Java-object-based Rows (i.e. rows
   * that are not UnsafeRows).
    * 指定此运算符是否能够处理基于Java对象的行(即不是UnsafeRows的行)
   */
  def canProcessSafeRows: Boolean = true

  /**
   * Returns the result of this query as an RDD[InternalRow] by delegating to doExecute
   * after adding query plan information to created RDDs for visualization.
   * Concrete implementations of SparkPlan should override doExecute instead.
    * 通过在将查询计划信息添加到创建的RDD以进行可视化之后委派给doExecute,
    * 将此查询的结果作为RDD [InternalRow]返回,SparkPlan的具体实现应该覆盖doExecute
   */
  final def execute(): RDD[InternalRow] = {
    if (children.nonEmpty) {
      val hasUnsafeInputs = children.exists(_.outputsUnsafeRows)
      val hasSafeInputs = children.exists(!_.outputsUnsafeRows)
      assert(!(hasSafeInputs && hasUnsafeInputs),
        "Child operators should output rows in the same format")
      assert(canProcessSafeRows || canProcessUnsafeRows,
        "Operator must be able to process at least one row format")
      assert(!hasSafeInputs || canProcessSafeRows,
        "Operator will receive safe rows as input but cannot process safe rows")
      assert(!hasUnsafeInputs || canProcessUnsafeRows,
        "Operator will receive unsafe rows as input but cannot process unsafe rows")
    }
    RDDOperationScope.withScope(sparkContext, nodeName, false, true) {
      prepare()
      doExecute()
    }
  }

  /**
   * Prepare a SparkPlan for execution. It's idempotent.
    * 准备SparkPlan以执行,这是幂等的
   */
  final def prepare(): Unit = {
    if (prepareCalled.compareAndSet(false, true)) {
      doPrepare()
      children.foreach(_.prepare())
    }
  }

  /**
   * Overridden by concrete implementations of SparkPlan. It is guaranteed to run before any
   * `execute` of SparkPlan. This is helpful if we want to set up some state before executing the
   * query, e.g., `BroadcastHashJoin` uses it to broadcast asynchronously.
    *
    * 被SparkPlan的具体实现覆盖,它保证在SparkPlan的任何`execute`之前运行,如果我们想在执行查询之前设置一些状态,
    * 这很有用,例如,`BroadcastHashJoin`使用它来异步广播。
   *
   * Note: the prepare method has already walked down the tree, so the implementation doesn't need
   * to call children's prepare methods.
    * 注意:prepare方法已经沿着树走了下去,因此实现不需要调用children的prepare方法
   */
  protected def doPrepare(): Unit = {}

  /**
   * Overridden by concrete implementations of SparkPlan.
   * Produces the result of the query as an RDD[InternalRow]
    * 被SparkPlan.Produces的具体实现覆盖,查询结果为RDD [InternalRow]
   */
  protected def doExecute(): RDD[InternalRow]

  /**
   * Runs this query returning the result as an array.
    * 运行此查询将结果作为数组返回
   */
  def executeCollect(): Array[Row] = {
    execute().mapPartitions { iter =>
      val converter = CatalystTypeConverters.createToScalaConverter(schema)
      iter.map(converter(_).asInstanceOf[Row])
    }.collect()
  }

  /**
   * Runs this query returning the first `n` rows as an array.
    * 运行此查询,将第一行“n”行作为数组返回
   *
   * This is modeled after RDD.take but never runs any job locally on the driver.
    * 这是在RDD.take之后建模的,但从不在驱动程序上本地运行任何作业
   */
  def executeTake(n: Int): Array[Row] = {
    if (n == 0) {
      return new Array[Row](0)
    }

    val childRDD = execute().map(_.copy())

    val buf = new ArrayBuffer[InternalRow]
    val totalParts = childRDD.partitions.length
    var partsScanned = 0
    while (buf.size < n && partsScanned < totalParts) {
      // The number of partitions to try in this iteration. It is ok for this number to be
      // greater than totalParts because we actually cap it at totalParts in runJob.
      //此迭代中要尝试的分区数,这个数字可以大于totalParts,因为我们实际上将它限制在runJob中的totalParts。
      var numPartsToTry = 1
      if (partsScanned > 0) {
        // If we didn't find any rows after the first iteration, just try all partitions next.
        // Otherwise, interpolate the number of partitions we need to try, but overestimate it
        // by 50%.
        //如果我们在第一次迭代后没有找到任何行,则只需尝试下一个所有分区,
        //否则,插入我们需要尝试的分区数,但高估它50％。
        if (buf.size == 0) {
          numPartsToTry = totalParts - 1
        } else {
          numPartsToTry = (1.5 * n * partsScanned / buf.size).toInt
        }
      }
      numPartsToTry = math.max(0, numPartsToTry)  // guard against negative num of partitions

      val left = n - buf.size
      val p = partsScanned until math.min(partsScanned + numPartsToTry, totalParts)
      val sc = sqlContext.sparkContext
      val res =
        sc.runJob(childRDD, (it: Iterator[InternalRow]) => it.take(left).toArray, p)

      res.foreach(buf ++= _.take(n - buf.size))
      partsScanned += numPartsToTry
    }

    val converter = CatalystTypeConverters.createToScalaConverter(schema)
    buf.toArray.map(converter(_).asInstanceOf[Row])
  }

  private[this] def isTesting: Boolean = sys.props.contains("spark.testing")

  protected def newProjection(
      expressions: Seq[Expression], inputSchema: Seq[Attribute]): Projection = {
    log.debug(
      s"Creating Projection: $expressions, inputSchema: $inputSchema, codegen:$codegenEnabled")
    if (codegenEnabled) {
      try {
        GenerateProjection.generate(expressions, inputSchema)
      } catch {
        case e: Exception =>
          if (isTesting) {
            throw e
          } else {
            log.error("Failed to generate projection, fallback to interpret", e)
            new InterpretedProjection(expressions, inputSchema)
          }
      }
    } else {
      new InterpretedProjection(expressions, inputSchema)
    }
  }

  protected def newMutableProjection(
      expressions: Seq[Expression],
      inputSchema: Seq[Attribute]): () => MutableProjection = {
    log.debug(
      s"Creating MutableProj: $expressions, inputSchema: $inputSchema, codegen:$codegenEnabled")
    if(codegenEnabled) {
      try {
        GenerateMutableProjection.generate(expressions, inputSchema)
      } catch {
        case e: Exception =>
          if (isTesting) {
            throw e
          } else {
            log.error("Failed to generate mutable projection, fallback to interpreted", e)
            () => new InterpretedMutableProjection(expressions, inputSchema)
          }
      }
    } else {
      () => new InterpretedMutableProjection(expressions, inputSchema)
    }
  }

  protected def newPredicate(
      expression: Expression, inputSchema: Seq[Attribute]): (InternalRow) => Boolean = {
    if (codegenEnabled) {
      try {
        GeneratePredicate.generate(expression, inputSchema)
      } catch {
        case e: Exception =>
          if (isTesting) {
            throw e
          } else {
            log.error("Failed to generate predicate, fallback to interpreted", e)
            InterpretedPredicate.create(expression, inputSchema)
          }
      }
    } else {
      InterpretedPredicate.create(expression, inputSchema)
    }
  }

  protected def newOrdering(
      order: Seq[SortOrder],
      inputSchema: Seq[Attribute]): Ordering[InternalRow] = {
    if (codegenEnabled) {
      try {
        GenerateOrdering.generate(order, inputSchema)
      } catch {
        case e: Exception =>
          if (isTesting) {
            throw e
          } else {
            log.error("Failed to generate ordering, fallback to interpreted", e)
            new InterpretedOrdering(order, inputSchema)
          }
      }
    } else {
      new InterpretedOrdering(order, inputSchema)
    }
  }
  /**
   * Creates a row ordering for the given schema, in natural ascending order.
    * 按自然升序为给定模式创建行排序
   */
  protected def newNaturalAscendingOrdering(dataTypes: Seq[DataType]): Ordering[InternalRow] = {
    val order: Seq[SortOrder] = dataTypes.zipWithIndex.map {
      case (dt, index) => new SortOrder(BoundReference(index, dt, nullable = true), Ascending)
    }
    newOrdering(order, Seq.empty)
  }
}

private[sql] trait LeafNode extends SparkPlan {
  override def children: Seq[SparkPlan] = Nil
}

private[sql] trait UnaryNode extends SparkPlan {
  def child: SparkPlan

  override def children: Seq[SparkPlan] = child :: Nil

  override def outputPartitioning: Partitioning = child.outputPartitioning
}

private[sql] trait BinaryNode extends SparkPlan {
  def left: SparkPlan
  def right: SparkPlan

  override def children: Seq[SparkPlan] = Seq(left, right)
}
