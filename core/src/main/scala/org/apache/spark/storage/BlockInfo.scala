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

package org.apache.spark.storage

import java.util.concurrent.ConcurrentHashMap

private[storage] class BlockInfo(val level: StorageLevel, val tellMaster: Boolean) {
  // To save space, 'pending' and 'failed' are encoded as special sizes:
  //为了节省空间，“挂起”和“失败”被编码为特殊尺寸：
  @volatile var size: Long = BlockInfo.BLOCK_PENDING
  private def pending: Boolean = size == BlockInfo.BLOCK_PENDING
  private def failed: Boolean = size == BlockInfo.BLOCK_FAILED
  private def initThread: Thread = BlockInfo.blockInfoInitThreads.get(this)//根据当前对象获取当前线程对象

  setInitThread()//初始化对象

  private def setInitThread() {
    /* Set current thread as init thread - waitForReady will not block this thread
     * (in case there is non trivial initialization which ends up calling waitForReady
     * as part of initialization itself)
     * 将当前线程设置为init线程 - waitForReady不会阻止此线程（如果有非平凡的初始化，最终调用waitForReady作为初始化本身的一部分）*/
    BlockInfo.blockInfoInitThreads.put(this, Thread.currentThread())//
  }

  /**
   * 等待block完成写入
   * Wait for this BlockInfo to be marked as ready (i.e. block is finished writing).
   * Return true if the block is available, false otherwise.
   *等待该BlockInfo被标记为就绪（即块写完）。 如果块可用，返回true，否则返回false。
   */
  def waitForReady(): Boolean = {
    if (pending && initThread != Thread.currentThread()) {
      synchronized {
        while (pending) {
          this.wait()//线程等待
        }
      }
    }
    !failed
  }

  /** 
   *  Mark this BlockInfo as ready (i.e. block is finished writing)
   *  标记一个 BlockInfo准备,块完成正在写入中.
   *  */
  def markReady(sizeInBytes: Long) {
    require(sizeInBytes >= 0, s"sizeInBytes was negative: $sizeInBytes")
    assert(pending)
    size = sizeInBytes
    BlockInfo.blockInfoInitThreads.remove(this)
    synchronized {
      this.notifyAll()//用于通知处在等待该对象的线程的方法
    }
  }

  /** 
   *  Mark this BlockInfo as ready but failed 
   *  标记BlockInfo数据失败
   *  */
  def markFailure() {
    assert(pending)
    size = BlockInfo.BLOCK_FAILED
    //移除BlockInfo
    BlockInfo.blockInfoInitThreads.remove(this)
    synchronized {
      this.notifyAll()//用于通知处在等待该对象的线程的方法
    }
  }
}

private object BlockInfo {
  /* initThread is logically a BlockInfo field, but we store it here because
   * it's only needed while this block is in the 'pending' state and we want
   * to minimize BlockInfo's memory footprint.
   * initThread在逻辑上是一个BlockInfo字段,但是我们将其存储在这里,因为只有在该块处于'pending'状态时才需要它,
   * 并且我们希望最小化BlockInfo的内存占用。*/
  private val blockInfoInitThreads = new ConcurrentHashMap[BlockInfo, Thread]

  private val BLOCK_PENDING: Long = -1L //在等待…期间
  private val BLOCK_FAILED: Long = -2L//失败
}
