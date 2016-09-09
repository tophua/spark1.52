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
* 将map处理的结果，传输到reduce上的过程叫Shuffle
* 
* ShuflleMap 会产生临时计算结果,这些数据会被ResulTask 作为输入而读取.
* 
* ShuffleMapTask的计算结果是如何被ResultTask取得的呢?
* 1)ShuffleMapTask将计算的状态,包装为MapStatus返回给DAGScheduler
* 2)DAGScheduler将MapStatus保存到MapOutputTrackerMaster中
* 3)ResultTask在调用到ShuffleRDD时,会利用BlockStoreShuffleFetch的fetch方法去获取数据
*   1)第一件事情就是咨询MapOutputTrackMaster所要取的数据的location
*   2)根据返回的结果调用BlockManager.getMultiple获取真正的数据
* 每个ShuffleMapTask都会用一个MapStatus来保存计算结果
* MapStatus由BlockManagerId和byteSize构成,BlockManagerId表示这些计算的中间结果实际数据在那个BlockManager
* See [[org.apache.spark.scheduler.Task]] for more information.
*
* 
 * @param stageId id of the stage this task belongs to
 * @param taskBinary broadcast version of the RDD and the ShuffleDependency. Once deserialized,
 *                   the type should be (RDD[_], ShuffleDependency[_, _, _]).
 * @param partition partition of the RDD this task is associated with
 * @param locs preferred task execution locations for locality scheduling
 */
private[spark] class ShuffleMapTask(
    stageId: Int,
    stageAttemptId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    @transient private var locs: Seq[TaskLocation],
    internalAccumulators: Seq[Accumulator[Long]])
  extends Task[MapStatus](stageId, stageAttemptId, partition.index, internalAccumulators)
  with Logging {

  /** A constructor used only in test suites. This does not require passing in an RDD. */
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
    val deserializeStartTime = System.currentTimeMillis()
    val ser = SparkEnv.get.closureSerializer.newInstance()    
    // 反序列化广播变量taskBinary得到RDD,ShuffleDependency
    val (rdd, dep) = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    _executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime

    metrics = Some(context.taskMetrics)
    var writer: ShuffleWriter[Any, Any] = null
    try {
      val manager = SparkEnv.get.shuffleManager //获得Shuffle Manager      
      //根据partition指定分区的Shufflea获取Shuffle Writer,shuffleHandle是shuffle ID
      writer = manager.getWriter[Any, Any](dep.shuffleHandle, partitionId, context)
      //首先调用rdd .iterator，如果该RDD已经cache了或者checkpoint了，那么直接读取结果，
      //否则开始计算计算的结果将调用Shuffle Writer写入本地文件系统
      writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
     // 返回MapStatus数据的元数据信息，包括location和size
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
