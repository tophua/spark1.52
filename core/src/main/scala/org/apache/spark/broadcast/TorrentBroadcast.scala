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

import java.io._
import java.nio.ByteBuffer

import scala.collection.JavaConversions.asJavaEnumeration
import scala.reflect.ClassTag
import scala.util.Random

import org.apache.spark.{Logging, SparkConf, SparkEnv, SparkException}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.{BroadcastBlockId, StorageLevel}
import org.apache.spark.util.{ByteBufferInputStream, Utils}
import org.apache.spark.util.io.ByteArrayChunkOutputStream

/**
 * A BitTorrent-like implementation of [[org.apache.spark.broadcast.Broadcast]].
 * 一个BT实现
 * The mechanism is as follows:
 *
 * The driver divides the serialized object into small chunks and
 * stores those chunks in the BlockManager of the driver.
 * driver将序列化对象划分一个个小块,交给BlockManager处理存储,
 *
 * On each executor, the executor first attempts to fetch the object from its BlockManager. If
 * it does not exist, it then uses remote fetches to fetch the small chunks from the driver and/or
 * other executors if available. Once it gets the chunks, it puts the chunks in its own
 * BlockManager, ready for other executors to fetch from.
  *
  * 每一个执行器executor将首先尝试从BlockManager获取的对象,如果没有找到,然后使用远程从driver或者其他executor获取小数据块（如果可用）,
  * 一旦它得到的这个数据块,它会把块在自己的BlockManager,可供其他executors获取。
 *
 * This prevents the driver from being the bottleneck in sending out multiple copies of the
 * broadcast data (one per executor) as done by the [[org.apache.spark.broadcast.HttpBroadcast]].
  *
  * 这样可以防止驱动程序像[[org.apache.spark.broadcast.HttpBroadcast]]完成的发送广播数据的多个副本（每个执行者一个）的瓶颈。
 *
 * When initialized, TorrentBroadcast objects read SparkEnv.get.conf.
  * 当初始化时，TorrentBroadcast对象读取SparkEnv.get.conf。
 *
 * @param obj object to broadcast
 * @param id A unique identifier for the broadcast variable.
 */
private[spark] class TorrentBroadcast[T: ClassTag](obj: T, id: Long)
  extends Broadcast[T](id) with Logging with Serializable {

  /**
   * Value of the broadcast object on executors. This is reconstructed by [[readBroadcastBlock]],
   * which builds this value by reading blocks from the driver and/or other executors.
    * 广播对象在执行器上的值,这由[[readBroadcastBlock]]重建,通过读取驱动程序和/或其他执行程序的块来构建此值
   *
   * On the driver, if the value is required, it is read lazily from the block manager.
    * 在驱动程序上,如果需要该值,则会从块管理器中读取
   */
  @transient private lazy val _value: T = readBroadcastBlock()

  /** The compression codec to use, or None if compression is disabled
    * 要使用的压缩编解码器，如果压缩被禁用，则为无 */
  @transient private var compressionCodec: Option[CompressionCodec] = _
  /** Size of each block. Default value is 4MB.  This value is only read by the broadcaster.
    * 每个块的大小 默认值为4MB,该值仅由广播公司读取 */
  @transient private var blockSize: Int = _
  //设置广播配置信息,配置属性确认是否对广播消息进行压缩,并且生成CompressionCode对象
  private def setConf(conf: SparkConf) {
  //是否在发送之前压缩广播变量
    compressionCodec = if (conf.getBoolean("spark.broadcast.compress", true)) {
      Some(CompressionCodec.createCodec(conf))
    } else {
      None
    }
    // Note: use getSizeAsKb (not bytes) to maintain compatiblity if no units are provided
    //根据配置属性设置块大小,默认4M
    blockSize = conf.getSizeAsKb("spark.broadcast.blockSize", "4m").toInt * 1024
  }
  
  setConf(SparkEnv.get.conf)
  //生成BroadcastBlockId
  private val broadcastId = BroadcastBlockId(id)

  /** Total number of blocks this broadcast variable contains. */
  //块的写入操作,返回广播变更包含的块数,
  private val numBlocks: Int = writeBlocks(obj)

  override protected def getValue() = {
    _value
  }

  /**
   * Divide the object into multiple blocks and put those blocks in the block manager.
   * 将该对象划分为多个块,并将这些块放在块管理器中
   * @param value the object to divide
   * @return number of blocks this broadcast variable is divided into
   */
  private def writeBlocks(value: T): Int = {
    // Store a copy of the broadcast variable in the driver so that tasks run on the driver
    // do not create a duplicate copy of the broadcast variable's value.
    //将一个广播变量的副本存储在驱动程序中，以便在驱动程序上运行的任务不创建广播变量值的重复副本。
    //1)将要写入的对象在本地的存储体系中备份一份,以便于Task也可以在本地的Driver上运行
    SparkEnv.get.blockManager.putSingle(broadcastId, value, StorageLevel.MEMORY_AND_DISK,
      tellMaster = false)
  //2)给ByteArrayChunkOutputStream指定压缩算法,并且将对象以序列化方式写入ByteChunkOutputStream后转换为Array
    val blocks =
      TorrentBroadcast.blockifyObject(value, blockSize, SparkEnv.get.serializer, compressionCodec)
    blocks.zipWithIndex.foreach { case (block, i) =>
      //3)将每一个ByteBuffer作为一个Block,使用putByte方法写入存储体系.
      SparkEnv.get.blockManager.putBytes(
        BroadcastBlockId(id, "piece" + i),
        block,
        StorageLevel.MEMORY_AND_DISK_SER,
        tellMaster = true)
    }
    blocks.length
  }

  /** Fetch torrent blocks from the driver and/or other executors.
    * 从驱动程序和/或其他执行程序提取洪流块*/
  private def readBlocks(): Array[ByteBuffer] = {
    // Fetch chunks of data. Note that all these chunks are stored in the BlockManager and reported
    // to the driver, so other executors can pull these chunks from this executor as well.
    //获取到的block被存在本地的BlockManager中并且上报给driver,这样其它的executor就可以从这个executor获取这些block了
    val blocks = new Array[ByteBuffer](numBlocks)
    val bm = SparkEnv.get.blockManager
    //需要shuffle,避免所有executor以同样的顺序下载block,使得driver依然是瓶颈
    for (pid <- Random.shuffle(Seq.range(0, numBlocks))) {
      val pieceId = BroadcastBlockId(id, "piece" + pid)//组装BroadcastBlockId
      logDebug(s"Reading piece $pieceId of $broadcastId")
      // First try getLocalBytes because there is a chance that previous attempts to fetch the
      // broadcast blocks have already fetched some of the blocks. In that case, some blocks
      // would be available locally (on this executor).
      //先试着从本地获取,因为之前的尝试可能已经获取了一些block
      def getLocal: Option[ByteBuffer] = bm.getLocalBytes(pieceId)
      def getRemote: Option[ByteBuffer] = bm.getRemoteBytes(pieceId).map { block =>
        // If we found the block from remote executors/driver's BlockManager, put the block
        // in this executor's BlockManager.
         //如果从remote获取了block,就把它存在本地的BlockManager
        SparkEnv.get.blockManager.putBytes(
          pieceId,
          block,
          StorageLevel.MEMORY_AND_DISK_SER,
          tellMaster = true)
        block
      }
      val block: ByteBuffer = getLocal.orElse(getRemote).getOrElse(
        throw new SparkException(s"Failed to get $pieceId of $broadcastId"))
      blocks(pid) = block
    }
    blocks
  }

  /**
   * Remove all persisted state associated with this Torrent broadcast on the executors.
   * 删除广播变量所有在executors端的持续状态
   */
  override protected def doUnpersist(blocking: Boolean) {
    TorrentBroadcast.unpersist(id, removeFromDriver = false, blocking)
  }

  /**
   * Remove all persisted state associated with this Torrent broadcast on the executors
   * and driver.
   * 删除广播变量所有在executors和driver端的持续状态
   */
  override protected def doDestroy(blocking: Boolean) {
    TorrentBroadcast.unpersist(id, removeFromDriver = true, blocking)
  }

  /** 
   *  Used by the JVM when serializing this object.
   *  使用 JVM序列化该对象
   *  */
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    assertValid()
    out.defaultWriteObject()
  }

  private def readBroadcastBlock(): T = Utils.tryOrIOException {
    TorrentBroadcast.synchronized {
      setConf(SparkEnv.get.conf)
       //从本地的blockManager里读这个被broadcast的对象,根据broadcastId
      SparkEnv.get.blockManager.getLocal(broadcastId).map(_.data.next()) match {
        case Some(x) => //本地有
          x.asInstanceOf[T]

        case None => //本地无
          logInfo("Started reading broadcast variable " + id)
          val startTimeMs = System.currentTimeMillis()
          val blocks = readBlocks()//如果本地没有broadcastId对应的broadcast的block,就读
          logInfo("Reading broadcast variable " + id + " took" + Utils.getUsedTimeMs(startTimeMs))

          val obj = TorrentBroadcast.unBlockifyObject[T](
            blocks, SparkEnv.get.serializer, compressionCodec)
          // Store the merged copy in BlockManager so other tasks on this executor don't
          // need to re-fetch it.
          //将合并的副本存储在BlockManager中,以便此执行器上的其他任务不需要重新获取
          SparkEnv.get.blockManager.putSingle(//读了之后再放进BlockManager
            broadcastId, obj, StorageLevel.MEMORY_AND_DISK, tellMaster = false)
          obj
      }
    }
  }

}


private object TorrentBroadcast extends Logging {
/**
 *blockifyObject方法用于将对象序列化写入ByteChunkOutputStream并用compressionCodec压缩,
      最终将ByteChunkOutputStream转换为Array[ByteBuffer]
 */
  def blockifyObject[T: ClassTag](
      obj: T,
      blockSize: Int,//块的大小
      serializer: Serializer,
      compressionCodec: Option[CompressionCodec]): Array[ByteBuffer] = {
    val bos = new ByteArrayChunkOutputStream(blockSize)//分块大小
    val out: OutputStream = compressionCodec.map(c => c.compressedOutputStream(bos)).getOrElse(bos)
    val ser = serializer.newInstance()
    val serOut = ser.serializeStream(out)
    serOut.writeObject[T](obj).close()
    bos.toArrays.map(ByteBuffer.wrap)
  }

  def unBlockifyObject[T: ClassTag](
      blocks: Array[ByteBuffer],
      serializer: Serializer,
      compressionCodec: Option[CompressionCodec]): T = {
    require(blocks.nonEmpty, "Cannot unblockify an empty array of blocks")
    val is = new SequenceInputStream(
      asJavaEnumeration(blocks.iterator.map(block => new ByteBufferInputStream(block))))
    val in: InputStream = compressionCodec.map(c => c.compressedInputStream(is)).getOrElse(is)
    val ser = serializer.newInstance()
    val serIn = ser.deserializeStream(in)
    val obj = serIn.readObject[T]()
    serIn.close()
    obj
  }

  /**
   * Remove all persisted blocks associated with this torrent broadcast on the executors.
   * If removeFromDriver is true, also remove these persisted blocks on the driver.
    * 删除与这个洪流播放相关的所有持久的块在执行器上,如果removeFromDriver为true，那么也可以删除驱动程序上的这些持久化块
   */
  def unpersist(id: Long, removeFromDriver: Boolean, blocking: Boolean): Unit = {
    logDebug(s"Unpersisting TorrentBroadcast $id")
    SparkEnv.get.blockManager.master.removeBroadcast(id, removeFromDriver, blocking)
  }
}
