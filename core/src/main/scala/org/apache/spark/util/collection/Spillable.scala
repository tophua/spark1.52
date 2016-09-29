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
  //读取最新输入溢出的元素数
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
  //开始跟踪的内存使用之前,初始化一个集合大小的值5M
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
   * @param currentMemory estimated size of the collection in bytes 估计的集合的大小
   * @return true if `collection` was spilled to disk; false otherwise,如果true,集合溢出到磁盘,否则
   */
  protected def maybeSpill(collection: C, currentMemory: Long): Boolean = {  
    if (elementsRead % 32 == 0 && currentMemory >= myMemoryThreshold) {
      // Claim up to double our current memory from the shuffle memory pool
     // 要求从shuffle内存池增加一倍内存
      val amountToRequest = 2 * currentMemory - myMemoryThreshold
      //为当前线程尝试获取amountToRequest大小的内存 
      val granted = shuffleMemoryManager.tryToAcquire(amountToRequest)      
      myMemoryThreshold += granted
      //如果获得的内存依然不足,内存不足可能是申请到的内存为0或者已经申请得到的内存大小超过myMemoryThreshold
      if (myMemoryThreshold <= currentMemory) {
        // We were granted too little memory to grow further (either tryToAcquire returned 0,
        // or we already had more memory than myMemoryThreshold); spill the current collection
        //给予太少的内存,进一步增长,溢出当前内存
        _spillCount += 1
        logSpillage(currentMemory)
        //spill执行溢出操作,将内存中的数据溢出到分区文件
        spill(collection)
        
        _elementsRead = 0
        // Keep track of spills, and release memory 
        //已溢出内存字节数_memoryBytesSpilled增加线程当前内存大小
        _memoryBytesSpilled += currentMemory
        //释放当前线程占用的内存
        releaseMemoryForThisThread()
        return true
      }
    }
    false
  }

  /**
   * @return number of bytes spilled in total
   * 返回溢出的总字节数
   */
  def memoryBytesSpilled: Long = _memoryBytesSpilled

  /**
   * Release our memory back to the shuffle pool so that other threads can grab it.
   * 释放内存回到洗牌池,让其他线程可以抢占它
   */
  private def releaseMemoryForThisThread(): Unit = {
    // The amount we requested does not include the initial memory tracking threshold
    //我们所请求的数量不包括初始内存跟踪值
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
