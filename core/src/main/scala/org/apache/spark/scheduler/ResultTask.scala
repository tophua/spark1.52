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

import java.io._

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
 * A task that sends back the output to the driver application.
 *对于最后一个Stage,会根据生成结果的Partition来生成与Partition数量相同的ResultTask
 * 然后ResultTask会将计算的结果汇报到Driver端
 * See [[Task]] for more information.
 *
 * @param stageId id of the stage this task belongs to 该任务所属的阶段的ID
 * @param taskBinary broadcasted version of the serialized RDD and the function to apply on each
 *                   partition of the given RDD. Once deserialized, the type should be
 *                   (RDD[T], (TaskContext, Iterator[T]) => U).
  *                   播放版本的序列化RDD以及应用于给定RDD的每个分区的功能,
  *                   反序列化后,类型应为(RDD [T],(TaskContext，Iterator [T]）=> U)
 * @param partition partition of the RDD this task is associated with 该任务与RDD的分区
 * @param locs preferred task execution locations for locality scheduling 用于地点调度的首选任务执行位置
 * @param outputId index of the task in this job (a job can launch tasks on only a subset of the
 *                 input RDD's partitions).
  *                 此作业中任务的索引(作业只能在输入RDD的分区的子集上启动任务)
 */
private[spark] class ResultTask[T, U](
    stageId: Int,
    stageAttemptId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    @transient locs: Seq[TaskLocation],
    val outputId: Int,//
    internalAccumulators: Seq[Accumulator[Long]])
  extends Task[U](stageId, stageAttemptId, partition.index, internalAccumulators)
  with Serializable {

  @transient private[this] val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  override def runTask(context: TaskContext): U = {
    // Deserialize the RDD and the func using the broadcast variables.
     //获取反序列化的起始时间  
    val deserializeStartTime = System.currentTimeMillis()
     //获取反序列化器  
    val ser = SparkEnv.get.closureSerializer.newInstance()
    //调用反序列化器ser的deserialize()方法,得到RDD和FUNC,数据来自taskBinary  
    val (rdd, func) = ser.deserialize[(RDD[T], (TaskContext, Iterator[T]) => U)](
      //Thread.currentThread().getContextClassLoader,可以获取当前线程的引用,getContextClassLoader用来获取线程的上下文类加载器
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
     //计算反序列化时间_executorDeserializeTime  
    _executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime
    //Task的taskMetrics信息
    metrics = Some(context.taskMetrics)
   // 调针对RDD中的每个分区,迭代执行func方法,执行Task  
    func(context, rdd.iterator(partition, context))
  }

  // This is only callable on the driver side.
  //这只能在driver使用
  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString: String = "ResultTask(" + stageId + ", " + partitionId + ")"
}
