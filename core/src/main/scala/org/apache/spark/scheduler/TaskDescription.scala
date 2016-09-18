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

import org.apache.spark.util.SerializableBuffer

/**
 * Description of a task that gets passed onto executors to be executed, usually created by
 * [[TaskSetManager.resourceOffer]].
 * 描述一个任务被传递到executors执行,通常由TaskSetManager.resourceOffer创建
 */
private[spark] class TaskDescription(
    val taskId: Long,//任务ID
    val attemptNumber: Int,//失败尝试数
    val executorId: String,//ExecutID
    val name: String,//任务名称
    //在TaskSet的任务索引
    val index: Int,    // Index within this task's TaskSet
    _serializedTask: ByteBuffer)//task执行环境的依赖的信息
  extends Serializable {

  // Because ByteBuffers are not serializable, wrap the task in a SerializableBuffer
  //因为不可序列化的字节缓冲区
  private val buffer = new SerializableBuffer(_serializedTask)

  def serializedTask: ByteBuffer = buffer.value

  override def toString: String = "TaskDescription(TID=%d, index=%d)".format(taskId, index)
}
