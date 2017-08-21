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

import java.io.{File, FileWriter}

import scala.language.reflectiveCalls

import org.mockito.Mockito.{mock, when}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.util.Utils
//DiskBlockManager管理和维护了逻辑上的Block和存储在Disk上的物理的Block的映射。
//一般来说,一个逻辑的Block会根据它的BlockId生成的名字映射到一个物理上的文件
class DiskBlockManagerSuite extends SparkFunSuite with BeforeAndAfterEach with BeforeAndAfterAll {
  private val testConf = new SparkConf(false)
  private var rootDir0: File = _
  private var rootDir1: File = _
  private var rootDirs: String = _

  val blockManager = mock(classOf[BlockManager])
  when(blockManager.conf).thenReturn(testConf)
  //DiskBlockManager创建和维护逻辑块和物理磁盘位置之间的逻辑映射,默认情况下,一个块被映射到一个文件,其名称由其BlockId给出
  var diskBlockManager: DiskBlockManager = _

  override def beforeAll() {
    super.beforeAll()
    rootDir0 = Utils.createTempDir()
    rootDir1 = Utils.createTempDir()
    rootDirs = rootDir0.getAbsolutePath + "," + rootDir1.getAbsolutePath
  }

  override def afterAll() {
    super.afterAll()
    Utils.deleteRecursively(rootDir0)
    Utils.deleteRecursively(rootDir1)
  }

  override def beforeEach() {
    val conf = testConf.clone
    conf.set("spark.local.dir", rootDirs)
    diskBlockManager = new DiskBlockManager(blockManager, conf)
  }

  override def afterEach() {
    diskBlockManager.stop()
  }

  test("basic block creation") {//基本块的创建
    val blockId = new TestBlockId("test")
    //DiskBlockManager创建和维护逻辑块和物理磁盘位置之间的逻辑映射,默认情况下,一个块被映射到一个文件,其名称由其BlockId给出
    val newFile = diskBlockManager.getFile(blockId)
    writeToFile(newFile, 10)
    assert(diskBlockManager.containsBlock(blockId))
    newFile.delete()
    assert(!diskBlockManager.containsBlock(blockId))
  }

  test("enumerating blocks") {//枚举块
    val ids = (1 to 100).map(i => TestBlockId("test_" + i))
    val files = ids.map(id => diskBlockManager.getFile(id))
    files.foreach(file => writeToFile(file, 10))
    assert(diskBlockManager.getAllBlocks.toSet === ids.toSet)
  }

  def writeToFile(file: File, numBytes: Int) {
    val writer = new FileWriter(file, true)
    for (i <- 0 until numBytes) writer.write(i)
    writer.close()
  }
}
