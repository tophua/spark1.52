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

import scala.reflect.ClassTag

import org.apache.spark.Partition

/**
 * Enumeration to manage state transitions of an RDD through checkpointing
 * 枚举通过检查点管理一个RDD状态转换
 * [ Initialized --> checkpointing in progress(检查点进程) --> checkpointed ].
 */
private[spark] object CheckpointState extends Enumeration {
  type CheckpointState = Value //type关键字,用来给类型或者是操作起别名,用起来很是方便 ,Value扩展枚举类型
  val Initialized, CheckpointingInProgress, Checkpointed = Value
}

/**
 * 这个类包含所有RDD检查点相关信息
 * This class contains all the information related to RDD checkpointing. Each instance of this
 * class is associated with a RDD. It manages process of checkpointing of the associated RDD,
 * as well as, manages the post-checkpoint state by providing the updated partitions,
 * iterator and preferred locations of the checkpointed RDD.
  * 此类包含与RDD检查点相关的所有信息,该类的每个实例都与RDD相关联,
  * 它管理关联的RDD的检查点的过程,以及通过提供检查点RDD的更新的分区,迭代器和首选位置来管理后检查点状态。
 */
private[spark] abstract class RDDCheckpointData[T: ClassTag](@transient rdd: RDD[T])
  extends Serializable {

  import CheckpointState._

  // The checkpoint state of the associated RDD . RDD检查点初始状态
  protected var cpState = Initialized

  // The RDD that contains our checkpointed data,包含RDD检查点的数据
  private var cpRDD: Option[CheckpointRDD[T]] = None //如果返回的是None,则代表没有给值

  // TODO: are we sure we need to use a global lock in the following methods?

  /**
   * 返回RDD检查点是否已经持久化
   * Return whether the checkpoint data for this RDD is already persisted.
   */
  def isCheckpointed: Boolean = RDDCheckpointData.synchronized {
    cpState == Checkpointed
  }

  /**
   * 保存RDD内容持久化
   * Materialize this RDD and persist its content.
   * This is called immediately after the first action invoked on this RDD has completed.
    * 在此RDD调用的第一个操作完成后立即调用
   */
  final def checkpoint(): Unit = {
    // Guard against multiple threads checkpointing the same RDD by
    // atomically flipping the state of this RDDCheckpointData
    //防止多线程通过原子地翻转此RDDCheckpointData的状态来检查同一个RDD
    RDDCheckpointData.synchronized {
      if (cpState == Initialized) {
        cpState = CheckpointingInProgress //检查点处理中
      } else {
        return
      }
    }

    val newRDD = doCheckpoint()

    // Update our state and truncate the RDD lineage
    //更新我们的状态并截断RDD谱系
    RDDCheckpointData.synchronized {
      cpRDD = Some(newRDD) //当回传Some的时候,代表这个函式成功地给了值
      cpState = Checkpointed//检查点保存完成
      rdd.markCheckpointed()//清除原始RDD和partitions的依赖
    }
  }

  /**
   * Materialize this RDD and persist its content.
   * RDD内容的检查点持久化
   * Subclasses should override this method to define custom checkpointing behavior.
    * 子类应该覆盖此方法来定义自定义检查点行为
   * @return the checkpoint RDD created in the process.
   */
  protected def doCheckpoint(): CheckpointRDD[T]

  /**
   * 返回RDD包含的检查点数据
   * Return the RDD that contains our checkpointed data.
   * This is only defined if the checkpoint state is `Checkpointed`.
    * 只有检查点状态为“Checkpointed”时才定义
   */
  def checkpointRDD: Option[CheckpointRDD[T]] = RDDCheckpointData.synchronized { cpRDD }

  /**
   * Return the partitions of the resulting checkpoint RDD.
   * 返回检查点RDD的分区。
   * For tests only.
   */
  def getPartitions: Array[Partition] = RDDCheckpointData.synchronized {
    cpRDD.map(_.partitions).getOrElse { Array.empty }
  }

}

/**
 * Global lock for synchronizing checkpoint operations.
 * 全局检查点同步锁对象
 */
private[spark] object RDDCheckpointData
