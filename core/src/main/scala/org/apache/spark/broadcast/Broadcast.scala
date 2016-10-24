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

package org.apache.spark.broadcast

import java.io.Serializable

import org.apache.spark.SparkException
import org.apache.spark.Logging
import org.apache.spark.util.Utils

import scala.reflect.ClassTag

/**
 * A broadcast variable. Broadcast variables allow the programmer to keep a read-only variable
 * 广播变量,广播变量允许程序保持一个只读变量缓存在每台机器上,而不是传递它的一个副本到任务,
 * cached on each machine rather than shipping a copy of it with tasks. They can be used, for
 * 他们可以使用例子,给每个节点复制一个大的输入数据集的一个有效的方式
 * example, to give every node a copy of a large input dataset in an efficient manner. Spark also
 * Spark还尝试使用高效的广播算法来分配广播变量，以降低通信成本
 * attempts to distribute broadcast variables using efficient broadcast algorithms to reduce
 * communication cost.
 *
 * Broadcast variables are created from a variable `v` by calling
 * 创建广播变量从一个变量V的调用
 * [[org.apache.spark.SparkContext#broadcast]].
 * The broadcast variable is a wrapper around `v`, and its value can be accessed by calling the
 * 广播变量是一个围绕“v”的包装器,它的值可以通过调用“value”方法来访问,
 * `value` method. The interpreter session below shows this: 
 * 下面的解释器会话显示了这个
 *
 * {{{
 * scala> val broadcastVar = sc.broadcast(Array(1, 2, 3))
 * broadcastVar: org.apache.spark.broadcast.Broadcast[Array[Int]] = Broadcast(0)
 *
 * scala> broadcastVar.value
 * res0: Array[Int] = Array(1, 2, 3)
 * }}}
 *
 * After the broadcast variable is created, it should be used instead of the value `v` in any
 * 创建广播变量后,它应该被任何运行在集群的方法使用值的“V”，“V”值不止一次传递节点,
 * functions run on the cluster so that `v` is not shipped to the nodes more than once.
 * In addition, the object `v` should not be modified after it is broadcast in order to ensure
 * 此外,广播后不应该修改对象的v,以确保所有的节点得到相同的广播变量的值
 * that all nodes get the same value of the broadcast variable (e.g. if the variable is shipped
 * 如果变量是运到一个新的节点后
 * to a new node later).
 *
 * @param id A unique identifier for the broadcast variable. 广播变量的唯一标识符
 * @tparam T Type of the data contained in the broadcast variable.广播变量中包含的数据的类型
 */
abstract class Broadcast[T: ClassTag](val id: Long) extends Serializable with Logging {

  /**
   * Flag signifying whether the broadcast variable is valid
   * 标示广播变量是否有效
   * (that is, not already destroyed) or not.
   */
  @volatile private var _isValid = true

  private var _destroySite = ""

  /** 
   *  Get the broadcasted value.
   *  获得广播变量的值 
   *  */
  def value: T = {
    assertValid()
    getValue()
  }

  /**
   * Asynchronously delete cached copies of this broadcast on the executors.
   * 在执行者异步删除缓存广播副本
   * If the broadcast is used after this is called, it will need to be re-sent to each executor.
   * 如果这个广播被调用后使用,它将需要重新发送给每一个执行者
   */
  def unpersist() {
    unpersist(blocking = false)
  }

  /**
   * Delete cached copies of this broadcast on the executors. If the broadcast is used after
   * 在执行删除缓存副本这个广播, 如果这个广播被调用后使用,它将需要重新发送给每一个执行者
   * this is called, it will need to be re-sent to each executor.
   * @param blocking Whether to block until unpersisting has completed 是否要等到unpersisting已完成
   */
  def unpersist(blocking: Boolean) {
    assertValid()
    doUnpersist(blocking)
  }


  /**
   * Destroy all data and metadata related to this broadcast variable. Use this with caution;
   * 销毁与此广播变量相关的所有数据和元数据,谨慎使用
   * once a broadcast variable has been destroyed, it cannot be used again.
   * 一旦一个广播变量被销毁,它不能再次使用
   * This method blocks until destroy has completed
   * 此方法块,直到销毁已完成
   */
  def destroy() {
    destroy(blocking = true)
  }

  /**
   * Destroy all data and metadata related to this broadcast variable. Use this with caution;
   * 销毁与此广播变量相关的所有数据和元数据,谨慎使用
   * once a broadcast variable has been destroyed, it cannot be used again.
   *  一旦一个广播变量被销毁,它不能再次使用
   * @param blocking Whether to block until destroy has completed 是否要等到unpersisting已完成
   */
  private[spark] def destroy(blocking: Boolean) {
    assertValid()
    _isValid = false
    _destroySite = Utils.getCallSite().shortForm
    logInfo("Destroying %s (from %s)".format(toString, _destroySite))
    doDestroy(blocking)
  }

  /**
   * Whether this Broadcast is actually usable. This should be false once persisted state is
   * 是否这个广播实际上是可用,一旦是假的,持久化状态被驱动程序删除
   * removed from the driver.
   */
  private[spark] def isValid: Boolean = {
    _isValid
  }

  /**
   * Actually get the broadcasted value. Concrete implementations of Broadcast class must
   * 获得广播变量实际值,具体实现广播类必须定义自定义的get方式来获得值
   * define their own way to get the value.
   */
  protected def getValue(): T

  /**
   * Actually unpersist the broadcasted value on the executors. Concrete implementations of
   * 执行者执行未持久化广播变量的值,具体实现广播类都必须自定义逻辑unpersist的数据
   * Broadcast class must define their own logic to unpersist their own data.
   */
  protected def doUnpersist(blocking: Boolean)

  /**
   * Actually destroy all data and metadata related to this broadcast variable.
   * 实际上销毁与广播变量相关的所有数据和元数据
   * Implementation of Broadcast class must define their own logic to destroy their own
   * 实现广播类必须自定义逻辑来销毁自己的状态
   * state.
   */
  protected def doDestroy(blocking: Boolean)

  /** 
   *  Check if this broadcast is valid. If not valid, exception is thrown.
   *  检查此广播是否有效,如果无效则抛出异常
   *   */
  protected def assertValid() {
    if (!_isValid) {
      throw new SparkException(
        "Attempted to use %s after it was destroyed (%s) ".format(toString, _destroySite))
    }
  }

  override def toString: String = "Broadcast(" + id + ")"
}