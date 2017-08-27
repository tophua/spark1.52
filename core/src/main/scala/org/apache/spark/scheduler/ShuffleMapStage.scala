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

import org.apache.spark.ShuffleDependency
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.CallSite

/**
 * The ShuffleMapStage represents the intermediate stages in a job.
 * ShuffleMapStage表示作业中的中间阶段(Stage)
  * ShuffleMapStage(ShuffleMapTask 所在的 stage)
 */
private[spark] class ShuffleMapStage(
    id: Int,
    rdd: RDD[_],
    numTasks: Int,
    parents: List[Stage],
    firstJobId: Int,
    callSite: CallSite,
    val shuffleDep: ShuffleDependency[_, _, _])
  extends Stage(id, rdd, numTasks, parents, firstJobId, callSite) {

  override def toString: String = "ShuffleMapStage " + id
   //输出计算任务数
  var numAvailableOutputs: Long = 0
  //通过判断numAvailableOutputs和numPartitions是否相等来确定stage是否已被提交
  //判断输出计算任务数和分区数一样,即所有分区上的子任务都已完成
  //如果map stage已就绪的话返回true,即所有分区均有shuffle输出。这个将会和outputLocs.contains保持一致。 
  def isAvailable: Boolean = numAvailableOutputs == numPartitions
   //如果Map任务记录每个Parttion的MapStatus(包括执行的Task的BlankManager地址和要传给reduce任务的Block的估算大小)
   //Nil是一个空的List,::向队列的头部追加数据,创造新的列表
  val outputLocs = Array.fill[List[MapStatus]](numPartitions)(Nil)
  //MapStatus包括执行Task的BlockManager的地址和要传给reduce任务的Block的估算大小
  def addOutputLoc(partition: Int, status: MapStatus): Unit = {
    val prevList = outputLocs(partition)
    //Nil是一个空的List,::向队列的头部追加数据,创造新的列表
    outputLocs(partition) = status :: prevList //元素合并进List用::
    if (prevList == Nil) {//Nil表示空列表
      numAvailableOutputs += 1//输出任务自增1
    }
  }

  def removeOutputLoc(partition: Int, bmAddress: BlockManagerId): Unit = {
    val prevList = outputLocs(partition)
    val newList = prevList.filterNot(_.location == bmAddress)
    outputLocs(partition) = newList
    if (prevList != Nil && newList == Nil) {//Nil表示空列表
      numAvailableOutputs -= 1//输出任务自减1
    }
  }

  /**
   * Removes all shuffle outputs associated with this executor. Note that this will also remove
   * outputs which are served by an external shuffle server (if one exists), as they are still
   * registered with this execId.
    *
    * 删除与执行(executor)相关联的所有Shuffle输出,
    * 请注意,这也将被删除由外部随机服务器(如果存在)提供的输出,因为它们仍然注册到此execId。
   */
  def removeOutputsOnExecutor(execId: String): Unit = {
    var becameUnavailable = false
    for (partition <- 0 until numPartitions) {
      val prevList = outputLocs(partition)
      val newList = prevList.filterNot(_.location.executorId == execId)
      outputLocs(partition) = newList
      //Nil是一个空的List,::向队列的头部追加数据,创造新的列表
      if (prevList != Nil && newList == Nil) {
        becameUnavailable = true
        numAvailableOutputs -= 1
      }
    }
    if (becameUnavailable) {
      logInfo("%s is now unavailable on executor %s (%d/%d, %s)".format(
        this, execId, numAvailableOutputs, numPartitions, isAvailable))
    }
  }
}
