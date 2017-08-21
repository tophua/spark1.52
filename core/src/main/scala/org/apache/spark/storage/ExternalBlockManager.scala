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

/**
 * An abstract class that the concrete external block manager has to inherit.
  * 具体的外部块管理器必须继承的抽象类
 * The class has to have a no-argument constructor, and will be initialized by init,
 * which is invoked by ExternalBlockStore. The main input parameter is blockId for all
 * the methods, which is the unique identifier for Block in one Spark application.
 * 类必须有一个无参数的构造函数,并由init由InitialBlockStore调用初始化,主要输入参数是所有方法的blockId,
  * 它是一个Spark应用程序中块的唯一标识符。
  *
 * The underlying external block manager should avoid any name space conflicts  among multiple
 * Spark applications. For example, creating different directory for different applications
 * by randomUUID
  * 底层的外部块管理器应避免多个Spark应用程序之间的任何名称空间冲突,
  * 例如,通过randomUUID为不同的应用程序创建不同的目录
 *
 */
private[spark] abstract class ExternalBlockManager {

  protected var blockManager: BlockManager = _

  override def toString: String = {"External Block Store"}

  /**
   * Initialize a concrete block manager implementation. Subclass should initialize its internal
   * data structure, e.g, file system, in this function, which is invoked by ExternalBlockStore
   * right after the class is constructed. The function should throw IOException on failure
    * 初始化一个具体的块管理器实现,子类应该在此函数中初始化其内部数据结构,
    * 例如文件系统,这在构造类之后由ExternalBlockStore调用,该函数应该在失败时抛出IOException
   *
   * @throws java.io.IOException if there is any file system failure during the initialization.
    *                             如果在初始化期间有任何文件系统故障
    *
    * BlockManager会运行在Driver和每个Executor上,
    * 而运行在Driver上的BlockManger负责整个Job的Block的管理工作；
    * 运行在Executor上的BlockManger负责管理该Executor上的Block,并且向Driver的BlockManager汇报Block的信息和接收来自它的命令
    */

  def init(blockManager: BlockManager, executorId: String): Unit = {
    this.blockManager = blockManager
  }

  /**
   * Drop the block from underlying external block store, if it exists..
    * 从下面的外部块存储中删除块，如果它存在
   * @return true on successfully removing the block
   *         false if the block could not be removed as it was not found
    *         如果块没有找到,则无法将其删除,则成功删除块为true
   *
   * @throws java.io.IOException if there is any file system failure in removing the block.
    *                             如果有任何文件系统故障在删除块
   */
  def removeBlock(blockId: BlockId): Boolean

  /**
   * Used by BlockManager to check the existence of the block in the underlying external
   * block store.由BlockManager用于检查底层外部块存储中块的存在
   * @return true if the block exists.
   *         false if the block does not exists.
   *         如果块存在,则为true,如果块不存在,则为false。
   * @throws java.io.IOException if there is any file system failure in checking
   *                             the block existence.
    *                             如果有任何文件系统故障检查块存在
   */
  def blockExists(blockId: BlockId): Boolean

  /**
   * Put the given block to the underlying external block store. Note that in normal case,
   * putting a block should never fail unless something wrong happens to the underlying
   * external block store, e.g., file system failure, etc. In this case, IOException
   * should be thrown.
   * 将给定的块放入底层的外部块存储,请注意,在正常情况下,放置块不应该失败,除非底层外部块存储器发生错误,
    * 例如文件系统故障等。在这种情况下,应抛出IOException。
   * @throws java.io.IOException if there is any file system failure in putting the block.
    *                            如果有任何文件系统失败的块
   */
  def putBytes(blockId: BlockId, bytes: ByteBuffer): Unit

  def putValues(blockId: BlockId, values: Iterator[_]): Unit = {
    val bytes = blockManager.dataSerialize(blockId, values)
    putBytes(blockId, bytes)
  }

  /**
   * Retrieve the block bytes.检索块字节
   * @return Some(ByteBuffer) if the block bytes is successfully retrieved
   *         None if the block does not exist in the external block store.
    *         如果块字节被成功检索到,如果该块不存在于外部块存储中
   *
   * @throws java.io.IOException if there is any file system failure in getting the block.
    *                             如果有任何文件系统失败获取块
   */
  def getBytes(blockId: BlockId): Option[ByteBuffer]

  /**
   * Retrieve the block data. 检索块数据
   * @return Some(Iterator[Any]) if the block data is successfully retrieved
   *         None if the block does not exist in the external block store.
    *         如果块数据成功检索到,则该块在外部块存储中不存在
   *
   * @throws java.io.IOException if there is any file system failure in getting the block.
    *                             如果有任何文件系统失败获取块
   */
  def getValues(blockId: BlockId): Option[Iterator[_]] = {
    getBytes(blockId).map(buffer => blockManager.dataDeserialize(blockId, buffer))
  }

  /**
   * Get the size of the block saved in the underlying external block store,
   * which is saved before by putBytes.
    * 获取保存在底层的外块存储块的大小,这是保存在putbytes
   * @return size of the block 块体大小
   *         0 if the block does not exist 如果块不存在，则为0
   *
   * @throws java.io.IOException if there is any file system failure in getting the block size.
    *                            如果在获取块大小时有任何文件系统故障
   */
  def getSize(blockId: BlockId): Long

  /**
   * Clean up any information persisted in the underlying external block store,
    *清理底层外部块存储中保存的所有信息
   * e.g., the directory, files, etc,which is invoked by the shutdown hook of ExternalBlockStore
   * during system shutdown.
   * 例如,目录,文件,等等,这是通过对externalblockstore关机钩在系统关机。
   */
  def shutdown()
}
