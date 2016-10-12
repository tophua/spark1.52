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

import org.apache.spark.SparkFunSuite
import org.apache.spark.scheduler._

class BlockStatusListenerSuite extends SparkFunSuite {

  test("basic functions") {//基本功能
    val blockManagerId = BlockManagerId("0", "localhost", 10000)
    val listener = new BlockStatusListener()

    // Add a block manager and a new block status
    //添加一个块管理器和一个新的块状态
    listener.onBlockManagerAdded(SparkListenerBlockManagerAdded(0, blockManagerId, 0))
    listener.onBlockUpdated(SparkListenerBlockUpdated(
      BlockUpdatedInfo(
        blockManagerId,
        StreamBlockId(0, 100),
        StorageLevel.MEMORY_AND_DISK,
        memSize = 100,
        diskSize = 100,
        externalBlockStoreSize = 0)))
    // The new block status should be added to the listener
    //新的块状态应该被添加到侦听器中
    val expectedBlock = BlockUIData(
      StreamBlockId(0, 100),
      "localhost:10000",
      StorageLevel.MEMORY_AND_DISK,
      memSize = 100,
      diskSize = 100,
      externalBlockStoreSize = 0
    )
    val expectedExecutorStreamBlockStatus = Seq(
      ExecutorStreamBlockStatus("0", "localhost:10000", Seq(expectedBlock))
    )
    assert(listener.allExecutorStreamBlockStatus === expectedExecutorStreamBlockStatus)

    // Add the second block manager
    //添加第二块管理器
    val blockManagerId2 = BlockManagerId("1", "localhost", 10001)
    listener.onBlockManagerAdded(SparkListenerBlockManagerAdded(0, blockManagerId2, 0))
    // Add a new replication of the same block id from the second manager
    //从第二个管理器添加同一块标识的新复制
    listener.onBlockUpdated(SparkListenerBlockUpdated(
      BlockUpdatedInfo(
        blockManagerId2,
        StreamBlockId(0, 100),
        StorageLevel.MEMORY_AND_DISK,
        memSize = 100,
        diskSize = 100,
        externalBlockStoreSize = 0)))
    val expectedBlock2 = BlockUIData(
      StreamBlockId(0, 100),
      "localhost:10001",
      StorageLevel.MEMORY_AND_DISK,
      memSize = 100,
      diskSize = 100,
      externalBlockStoreSize = 0
    )
    // Each block manager should contain one block
    //每个块管理器都应该包含一个块
    val expectedExecutorStreamBlockStatus2 = Set(
      ExecutorStreamBlockStatus("0", "localhost:10000", Seq(expectedBlock)),
      ExecutorStreamBlockStatus("1", "localhost:10001", Seq(expectedBlock2))
    )
    assert(listener.allExecutorStreamBlockStatus.toSet === expectedExecutorStreamBlockStatus2)

    // Remove a replication of the same block
    //删除同一块的复制
    listener.onBlockUpdated(SparkListenerBlockUpdated(
      BlockUpdatedInfo(
        blockManagerId2,
        StreamBlockId(0, 100),
        StorageLevel.NONE, // StorageLevel.NONE means removing it
        memSize = 0,
        diskSize = 0,
        externalBlockStoreSize = 0)))
    // Only the first block manager contains a block
    //只有第一个块管理器包含一个块
    val expectedExecutorStreamBlockStatus3 = Set(
      ExecutorStreamBlockStatus("0", "localhost:10000", Seq(expectedBlock)),
      ExecutorStreamBlockStatus("1", "localhost:10001", Seq.empty)
    )
    assert(listener.allExecutorStreamBlockStatus.toSet === expectedExecutorStreamBlockStatus3)

    // Remove the second block manager at first but add a new block status
    // from this removed block manager
    //在第一次删除第二个块管理器,但从这个移除的块管理器中添加一个新的块状态
    listener.onBlockManagerRemoved(SparkListenerBlockManagerRemoved(0, blockManagerId2))
    listener.onBlockUpdated(SparkListenerBlockUpdated(
      BlockUpdatedInfo(
        blockManagerId2,
        StreamBlockId(0, 100),
        StorageLevel.MEMORY_AND_DISK,
        memSize = 100,
        diskSize = 100,
        externalBlockStoreSize = 0)))
    // The second block manager is removed so we should not see the new block
    //第二块管理器被删除,所以我们不应该看到新的块
    val expectedExecutorStreamBlockStatus4 = Seq(
      ExecutorStreamBlockStatus("0", "localhost:10000", Seq(expectedBlock))
    )
    assert(listener.allExecutorStreamBlockStatus === expectedExecutorStreamBlockStatus4)

    // Remove the last block manager
    //删除最后一个块管理器
    listener.onBlockManagerRemoved(SparkListenerBlockManagerRemoved(0, blockManagerId))
    // No block manager now so we should dop all block managers    
    assert(listener.allExecutorStreamBlockStatus.isEmpty)
  }

}
