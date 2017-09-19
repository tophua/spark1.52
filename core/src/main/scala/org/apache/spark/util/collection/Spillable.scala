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

package org.apache.spark.util.collection

import org.apache.spark.Logging
import org.apache.spark.SparkEnv

/**
 * Spills contents of an in-memory collection to disk when the memory threshold
 * has been exceeded.
 * 当内存阈值已超过时,将内存集合中的内存存储到磁盘上
 */
private[spark] trait Spillable[C] extends Logging {
  /**
   * Spills the current in-memory collection to disk, and releases the memory.
   * 将当前的内存存储到磁盘上,并释放内存
   * @param collection collection to spill to disk
   */
  protected def spill(collection: C): Unit

  // Number of elements read from input since last spill
  //已经读取过的元素个数
  protected def elementsRead: Long = _elementsRead

  // Called by subclasses every time a record is read
  // It's used for checking spilling frequency
  //每次一个记录被读取时调用,它用于检查溢出频率
  protected def addElementsRead(): Unit = { _elementsRead += 1 }

  // Memory manager that can be used to acquire/release memory
  //可以用来获取/释放内存的内存管理器
  private[this] val shuffleMemoryManager = SparkEnv.get.shuffleMemoryManager

  // Initial threshold for the size of a collection before we start tracking its memory usage
  // Exposed for testing
  //初始化一个集合大小的值5M
  private[this] val initialMemoryThreshold: Long =
    SparkEnv.get.conf.getLong("spark.shuffle.spill.initialMemoryThreshold", 5 * 1024 * 1024)

  // Threshold for this collection's size in bytes before we start tracking its memory usage
  // To avoid a large number of small spills, initialize this to a value orders of magnitude > 0
  //避免大量小泄漏,初始化值的数量级> 0
  private[this] var myMemoryThreshold = initialMemoryThreshold

  // Number of elements read from input since last spill
  //读取最新输入溢出的元素数
  private[this] var _elementsRead = 0L

  // Number of bytes spilled in total
  // 溢出的字节总数
  private[this] var _memoryBytesSpilled = 0L

  // Number of spills
  //溢出数
  private[this] var _spillCount = 0

  /**
   * Spills(溢出) the current in-memory collection to disk if needed. Attempts to acquire(获得) more
   * memory before spilling.
   * 如果需要将当前内存集合溢出到磁盘上,获得更多的内存
   * @param collection collection to spill to disk,集合溢出到磁盘
   * @param currentMemory estimated size of the collection in bytes 是一个预估值,表示当前占用的内存
   * @return true if `collection` was spilled to disk; false otherwise,如果true,集合溢出到磁盘,否则
   */
  protected def maybeSpill(collection: C, currentMemory: Long): Boolean = { 
    //如果我的内存阀值小于当前已使用的内存,则进行spill,否则不进行spill
    //elementsRead表示已经读取过的元素个数,只有当前读过的elements为32的整数倍才有可能spill 
    //currentMemory是一个预估值,表示当前占用的内存
    if (elementsRead % 32 == 0 && currentMemory >= myMemoryThreshold) {
      // Claim up to double our current memory from the shuffle memory pool
     // 申请的容量是当前使用容量*2减去内存阀值
      val amountToRequest = 2 * currentMemory - myMemoryThreshold
      //为当前线程尝试获取amountToRequest大小的内存 
      val granted = shuffleMemoryManager.tryToAcquire(amountToRequest)     
      //将申请的内存添加到能接受的内存阀值之上,即增加可忍受的内存阀值
      myMemoryThreshold += granted
      //此时的内存阀值还是小于当前使用两,则必须进行spill
      if (myMemoryThreshold <= currentMemory) {
        // We were granted too little memory to grow further (either tryToAcquire returned 0,
        // or we already had more memory than myMemoryThreshold); spill the current collection
        //给予太少的内存,进一步增长,溢出当前内存
        _spillCount += 1
        logSpillage(currentMemory)
        //spill执行溢出操作,将内存中的数据溢出到分区文件
        spill(collection)
        
        _elementsRead = 0//已读数清0
        // Keep track of spills, and release memory 
        //已经释放的内存总量
        _memoryBytesSpilled += currentMemory
        //spill到磁盘,释放已经占用的内存,将内存阀值恢复到最初值    
        releaseMemoryForThisThread()
        return true
      }
    }
    false //条件不满足,则不需要spill
  }

  /**
   * @return number of bytes spilled in total
   * 返回溢出的总字节数
   */
  def memoryBytesSpilled: Long = _memoryBytesSpilled

  /**
   * Release our memory back to the shuffle pool so that other threads can grab it.
   * 回收内存
   */
  private def releaseMemoryForThisThread(): Unit = {
    // The amount we requested does not include the initial memory tracking threshold
    //回收的内存总量,不能减去自身的大小
    shuffleMemoryManager.release(myMemoryThreshold - initialMemoryThreshold)
    myMemoryThreshold = initialMemoryThreshold
  }

  /**
   * Prints a standard log message detailing spillage.
   * 打印溢出标准日志消息
   * @param size number of bytes spilled
   */
  @inline private def logSpillage(size: Long) {
    val threadId = Thread.currentThread().getId
    logInfo("Thread %d spilling in-memory map of %s to disk (%d time%s so far)"
      .format(threadId, org.apache.spark.util.Utils.bytesToString(size),
        _spillCount, if (_spillCount > 1) "s" else ""))
  }
}
