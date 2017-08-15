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

import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.Logging

/**
 * Abstract class to store blocks.
  * 抽象类存储块
 */
private[spark] abstract class BlockStore(val blockManager: BlockManager) extends Logging {

  def putBytes(blockId: BlockId, bytes: ByteBuffer, level: StorageLevel): PutResult

  /**
   * Put in a block and, possibly, also return its content as either bytes or another Iterator.
   * This is used to efficiently write the values to multiple locations (e.g. for replication).
   * 将values写入系统,如果returnValue=true 需要将结果写入PutResult
    *放入一个块,并且也可能返回其内容作为字节或另一个迭代器,这用于有效地将值写入多个位置(例如用于复制)
   * @return a PutResult that contains the size of the data, as well as the values put if
   *         returnValues is true (if not, the result's data field can be null)
    *        包含数据大小的PutResult以及如果returnValues为true,则返回的值(如果不是,则结果的数据字段可以为空)
   */
  def putIterator(
    blockId: BlockId,
    values: Iterator[Any],
    level: StorageLevel,
    returnValues: Boolean): PutResult
/**
 * 根据StorageLevel将BlockId标识Block的内容bytes写入系统
 */
  def putArray(
    blockId: BlockId,
    values: Array[Any],
    level: StorageLevel,
    returnValues: Boolean): PutResult

  /**
   * 获得Block的大小
   * Return the size of a block in bytes.
   */
  def getSize(blockId: BlockId): Long
/**
 * 获得Block的数据,返回ByteBuffer
 */
  def getBytes(blockId: BlockId): Option[ByteBuffer]
/**
 * 获得Block的数据,返回Iterator
 */
  def getValues(blockId: BlockId): Option[Iterator[Any]]

  /**
   * 删除Block,成功返回true,否则返回false
   * Remove a block, if it exists.
   * @param blockId the block to remove.
   * @return True if the block was found and removed, False otherwise.
   */
  def remove(blockId: BlockId): Boolean
/**
 * 查询是否包含某个Block
 */
  def contains(blockId: BlockId): Boolean
/**
 * 退出时清理回收资源
 */
  def clear() { }
}
