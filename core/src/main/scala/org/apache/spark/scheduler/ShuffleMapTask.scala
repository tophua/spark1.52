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

package org.apache.spark.scheduler

import java.nio.ByteBuffer

import scala.language.existentials

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.ShuffleWriter

/**
 *
 * A ShuffleMapTask divides the elements of an RDD into multiple buckets (based on a partitioner
 * specified in the ShuffleDependency).
 * Spark的ShuffleMapTask和ResultTask被划分到不同Stage,ShuffleMapTask执行完毕将中间结果输出到本地磁盘系统(如HDFS)
 * 然后下一个Stage中的ResultTask通过ShuffleClient下载ShuffleMapTask的输出到本地磁盘
 *
 * 将map处理的结果,传输到reduce上的过程叫Shuffle
 * See [[org.apache.spark.scheduler.Task]] for more information.
 * @param stageId id of the stage this task belongs to
 * @param taskBinary broadcast version of the RDD and the ShuffleDependency. Once deserialized,
 *                   the type should be (RDD[_], ShuffleDependency[_, _, _]).
 * @param partition partition of the RDD this task is associated with
 * @param locs preferred task execution locations for locality scheduling
 */
private[spark] class ShuffleMapTask(
  stageId: Int,//stageId
  stageAttemptId: Int,//失败重试次数
  taskBinary: Broadcast[Array[Byte]],//task任务广播
  partition: Partition,//未计算的分区
  @transient private var locs: Seq[TaskLocation],//最佳调度执行的位置
  internalAccumulators: Seq[Accumulator[Long]])//内部累加器
    extends Task[MapStatus](stageId, stageAttemptId, partition.index, internalAccumulators)
    with Logging {

  /** A constructor used only in test suites. This does not require passing in an RDD.
    * 仅在测试套件中使用的构造函数,这不需要传递RDD*/
  def this(partitionId: Int) {
    this(0, 0, null, new Partition { override def index: Int = 0 }, null, null)
  }

  @transient private val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }
  /**
   *
   */
  override def runTask(context: TaskContext): MapStatus = {
    // Deserialize the RDD using the broadcast variable.   
    //反序列化的起始时间  
    val deserializeStartTime = System.currentTimeMillis()
    // 获得反序列化器closureSerializer  
    val ser = SparkEnv.get.closureSerializer.newInstance()
    // 调用反序列化器closureSerializer的deserialize()进行RDD和ShuffleDependency的反序列化,数据来源于taskBinary 
    val (rdd, dep) = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    //计算Executor进行反序列化的时间  
    _executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime

    metrics = Some(context.taskMetrics)
    var writer: ShuffleWriter[Any, Any] = null
    try {
      //获得shuffleManager
      val manager = SparkEnv.get.shuffleManager
      //根据partition指定分区的Shufflea获取Shuffle Writer,shuffleHandle是shuffle ID
      //partitionId表示的是当前RDD的某个partition,也就是说write操作作用于partition之上  
      writer = manager.getWriter[Any, Any](dep.shuffleHandle, partitionId, context)
      //针对RDD中的分区partition,调用rdd的iterator()方法后,再调用writer的write()方法,写数据  
      writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
      //停止writer,并返回标志位 
      writer.stop(success = true).get
    } catch {
      case e: Exception =>
        try {
          if (writer != null) {
            writer.stop(success = false)
          }
        } catch {
          case e: Exception =>
            log.debug("Could not stop writer", e)
        }
        throw e
    }
  }

  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString: String = "ShuffleMapTask(%d, %d)".format(stageId, partitionId)
}
