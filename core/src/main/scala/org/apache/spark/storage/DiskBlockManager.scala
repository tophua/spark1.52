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

import java.util.UUID
import java.io.{IOException, File}

import org.apache.spark.{SparkConf, Logging}
import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.util.{ShutdownHookManager, Utils}

/**
 * Creates and maintains the logical mapping between logical blocks and physical on-disk
 * locations. By default, one block is mapped to one file with a name given by its BlockId.
 * However, it is also possible to have a block map to only a segment of a file, by calling
 * mapBlockToFileSegment().
  *
  *创建和维护逻辑块和物理磁盘位置之间的逻辑映射,默认情况下,一个块被映射到一个文件,
  * 其名称由其BlockId给出。但是,也可以通过调用mapBlockToFileSegment（）将块映射到文件的一个段。
 * Block files are hashed among the directories listed in spark.local.dir (or in
 * SPARK_LOCAL_DIRS, if it's set).
  * 阻止文件在spark.local.dir中列出的目录中进行散列(或者在SPARK_LOCAL_DIRS中设置)
 */
private[spark] class DiskBlockManager(blockManager: BlockManager, conf: SparkConf)
  extends Logging {

  private[spark]
  //每个本地根目录生成子目录的个数,生成子目录是为了避免生成过多的索引节点
  val subDirsPerLocalDir = blockManager.conf.getInt("spark.diskStore.subDirectories", 64)

  /* Create one local directory for each path mentioned in spark.local.dir; then, inside this
   * directory, create multiple subdirectories that we will hash files into, in order to avoid
   * having really large inodes at the top level.
   * 为spark.local.dir中提到的每个路径创建一个本地目录,那么，在这个目录下,
   * 创建我们将哈希文件的多个子目录,以避免在顶层有真正的大型inode。*/
  //存放Block对应的File的本地根目录
  private[spark] val localDirs: Array[File] = createLocalDirs(conf)
  if (localDirs.isEmpty) {
    logError("Failed to create any local dir.")
    System.exit(ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR)
  }
  // The content of subDirs is immutable but the content of subDirs(i) is mutable. And the content
  // of subDirs(i) is protected by the lock of subDirs(i)
  //subDirs的内容是不可变的,但是subDirs(i)的内容是可变的,subDirs(i)的内容受到subDirs(i)的锁定的保护
  //存放所有子目录的二维数组
  private val subDirs = Array.fill(localDirs.length)(new Array[File](subDirsPerLocalDir))

  private val shutdownHook = addShutdownHook()

  /** Looks up a file by hashing it into one of our local subdirectories. */
  // This method should be kept in sync with
  //通过将文件散列成我们的本地子目录之一来查找文件,该方法应该保持同步
  // org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getFile().
  /**
   * 从磁盘获取文件,根据文件名,取得文件,该方法先将filename哈希到相应的子目录,然后判断子目录是否存在,若不存在则生成
   * 目录结构如下所述
   * /tmp/spark-local-20140723092540-7f24
   * /tmp/spark-local-20140723092540-7f24/0d
   * /tmp/spark-local-20140723092540-7f24/0d/shuffle0_0_0_1
   * /tmp/spark-local-20140723092540-7f24/0d/shuffle0_0_1_0
   * /tmp/spark-local-20140723092540-7f24/0c
   * /tmp/spark-local-20140723092540-7f24/0c/shuffle0_0_0_0
   * 
   */
  def getFile(filename: String): File = {
    // Figure out which local directory it hashes to, and which subdirectory in that
    //找出它所在的本地目录,以及哪个子目录
    val hash = Utils.nonNegativeHash(filename) //根据文件名称计算哈希值
    val dirId = hash % localDirs.length //根据哈希值与本地文件一级目录的总数余数,记为dirId
    val subDirId = (hash / localDirs.length) % subDirsPerLocalDir //根据哈希值与本地一级目录的总数求商数
    //此商数与二极目录的数目再求余数

    // Create the subdirectory if it doesn't already exist
    //创建子目录(如果不存在)
    val subDir = subDirs(dirId).synchronized {
      val old = subDirs(dirId)(subDirId) //如果dirId/subDir目录存在,则获取dirId/subDir目录下的文件
      if (old != null) {
        old
      } else {
        //否则新建dirId/subDir目录
        val newDir = new File(localDirs(dirId), "%02x".format(subDirId))
        if (!newDir.exists() && !newDir.mkdir()) {
          throw new IOException(s"Failed to create local dir in $newDir.")
        }
        subDirs(dirId)(subDirId) = newDir
        newDir
      }
    }

    new File(subDir, filename)
  }
  //根据BlockId取得相应的File
  def getFile(blockId: BlockId): File = getFile(blockId.name)

  /** 
   *  Check if disk block manager has a block. 
   *  判断BlockId是否有存储在该本地磁盘
   *  */
  def containsBlock(blockId: BlockId): Boolean = {
    getFile(blockId.name).exists()
  }

  /** 
   *  List all the files currently stored on disk by the disk manager. 
   *  取得存储的所有的文件
   *  */
  def getAllFiles(): Seq[File] = {
    // Get all the files inside the array of array of directories
    //获取目录数组中的所有文件
    subDirs.flatMap { dir =>
      dir.synchronized {
        // Copy the content of dir because it may be modified in other threads
        //复制数组目录的内容因为它可能在其他线程修改
        dir.clone()
      }
    }.filter(_ != null).flatMap { dir =>
      val files = dir.listFiles()
      if (files != null) files else Seq.empty
    }
  }

  /** 
   *  List all the blocks currently stored on disk by the disk manager. 
   *  取得存储的所有Block的BlockId。
   *  */
  def getAllBlocks(): Seq[BlockId] = {
    getAllFiles().map(f => BlockId(f.getName))
  }

  /** 
   *  Produces a unique block id and File suitable for storing local intermediate results.
    *  产生一个独特的块id和适用于存储局部中间结果的File
   *  创建本地临时文件
   * */
  
  def createTempLocalBlock(): (TempLocalBlockId, File) = {
    var blockId = new TempLocalBlockId(UUID.randomUUID())
    while (getFile(blockId).exists()) {
      blockId = new TempLocalBlockId(UUID.randomUUID())
    }
    (blockId, getFile(blockId))
  }

  /** 
   *  Produces a unique block id and File suitable for storing shuffled intermediate results. 
   *  当ShuffleMapTask运行结束需要把中间结果临时保存,此时就调用createTempShuffleBlock方法创建临时的Block
   *  文件返回TempShuffleBlockId与其文件的对偶
   *  */
  def createTempShuffleBlock(): (TempShuffleBlockId, File) = {
    var blockId = new TempShuffleBlockId(UUID.randomUUID())
    while (getFile(blockId).exists()) {
      //生成规则temp_shuffle_后加上UUID字符串
      blockId = new TempShuffleBlockId(UUID.randomUUID())
    }
    (blockId, getFile(blockId))
  }

  /**
   * Create local directories for storing block data. These directories are
   * located inside configured local directories and won't
   * be deleted on JVM exit when using the external shuffle service.
   * 创建用于存储块数据的本地目录,这些目录位于配置的本地目录内,并且在使用外部shuffle服务时不会在JVM退出时被删除。
    *
   * 创建本地文件目录,然后创建二维数组subDirs,用来缓存一级目录localDirs及二级目录
   * 其中二级目录的数量根据配置subDirsPerLocalDir获取,默认64.
   */
  private def createLocalDirs(conf: SparkConf): Array[File] = {
    Utils.getConfiguredLocalDirs(conf).flatMap { rootDir =>
      try {
        val localDir = Utils.createDirectory(rootDir, "blockmgr")
        logInfo(s"Created local directory at $localDir")
        Some(localDir)
      } catch {
        case e: IOException =>
          logError(s"Failed to create local dir in $rootDir. Ignoring this directory.", e)
          None
      }
    }
  }
   //添加运行时环境结束时的钩子,用于在进程关闭时创建线程
  private def addShutdownHook(): AnyRef = {
    ShutdownHookManager.addShutdownHook(ShutdownHookManager.TEMP_DIR_SHUTDOWN_PRIORITY + 1) { () =>
      logInfo("Shutdown hook called")
      //清除临时目录
      DiskBlockManager.this.doStop()
    }
  }

  /** 
   *  Cleanup local dirs and stop shuffle sender. 
   *  清理本地目录和发送停止shuffle信息。
   *  */
  private[spark] def stop() {

    // Remove the shutdown hook.  It causes memory leaks if we leave it around.
    //删除关机挂钩,如果我们离开它会导致内存泄漏,
    try {
      ShutdownHookManager.removeShutdownHook(shutdownHook)
    } catch {
      case e: Exception =>
        logError(s"Exception while removing shutdown hook.", e)
    }
    doStop()
  }
  //清除临时目录
  private def doStop(): Unit = {
    // Only perform cleanup if an external service is not serving our shuffle files.
    // Also blockManagerId could be null if block manager is not initialized properly.
    //如果外部服务不提供我们的随机播放文件,则只执行清理,如果块管理器未正确初始化,那么blockManagerId也可以为null。
    if (!blockManager.externalShuffleServiceEnabled ||
      (blockManager.blockManagerId != null && blockManager.blockManagerId.isDriver)) {
      localDirs.foreach { localDir =>
        if (localDir.isDirectory() && localDir.exists()) {
          try {
            if (!ShutdownHookManager.hasRootAsShutdownDeleteDir(localDir)) {
              Utils.deleteRecursively(localDir)
            }
          } catch {
            case e: Exception =>
              logError(s"Exception while deleting local spark dir: $localDir", e)
          }
        }
      }
    }
  }
}
