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

package org.apache.spark

import java.io._
import java.util.concurrent.ConcurrentHashMap
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Map}
import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv, RpcCallContext, RpcEndpoint}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.MetadataFetchFailedException
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockId}
import org.apache.spark.util._

private[spark] sealed trait MapOutputTrackerMessage
private[spark] case class GetMapOutputStatuses(shuffleId: Int)
  extends MapOutputTrackerMessage
private[spark] case object StopMapOutputTracker extends MapOutputTrackerMessage

/** RpcEndpoint class for MapOutputTrackerMaster */
private[spark] class MapOutputTrackerMasterEndpoint(
    override val rpcEnv: RpcEnv, tracker: MapOutputTrackerMaster, conf: SparkConf)
  extends RpcEndpoint with Logging {
  val maxAkkaFrameSize = AkkaUtils.maxFrameSizeBytes(conf)//返回Akka消息最大值
 //定义偏函数是具有类型PartialFunction[-A,+B]的一种函数。A是其接受的函数类型,B是其返回的结果类型
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case GetMapOutputStatuses(shuffleId: Int) =>
      //sender返回发送消息信息
      val hostPort = context.sender.address.hostPort
      //发送map任务输出指定机器
      logInfo("Asked to send map output locations for shuffle " + shuffleId + " to " + hostPort)
      
      val mapOutputStatuses = tracker.getSerializedMapOutputStatuses(shuffleId)
      val serializedSize = mapOutputStatuses.size
      if (serializedSize > maxAkkaFrameSize) {
      //Executor将结果回传到Driver时,大小首不能超过个机制设置的消息最大值
        val msg = s"Map output statuses were $serializedSize bytes which " +
          s"exceeds spark.akka.frameSize ($maxAkkaFrameSize bytes)."

        /* For SPARK-1244 we'll opt for just logging an error and then sending it to the sender.
         * A bigger refactoring (SPARK-1239) will ultimately remove this entire code path. */
        val exception = new SparkException(msg)
        logError(msg, exception)
        context.sendFailure(exception)
      } else {
        context.reply(mapOutputStatuses)
      }

    case StopMapOutputTracker =>
      logInfo("MapOutputTrackerMasterEndpoint stopped!")
      context.reply(true)
      stop()
  }
}

/**
 * Class that keeps track of the location of the map output of
 * a stage. This is abstract because different versions of MapOutputTracker
 * (driver and executor) use different HashMap to store its metadata.
 * 主要用于跟踪Map阶段任务的输出状态,此状态便于Reduce阶段任务获取地址及中间输出结果
 */
private[spark] abstract class MapOutputTracker(conf: SparkConf) extends Logging {

  /** Set to the MapOutputTrackerMasterEndpoint living on the driver.
    * 设置在驱动程序上的MapOutputTrackerMasterEndpoint*/
  var trackerEndpoint: RpcEndpointRef = _

  /**
   * This HashMap has different behavior for the driver and the executors.
    * 这个HashMap对驱动程序和执行程序有不同的行为

   * On the driver, it serves as the source of map outputs recorded from ShuffleMapTasks.
   * On the executors, it simply serves as a cache, in which a miss triggers a fetch from the
   * driver's corresponding HashMap.
   *  在驱动程序上,它作为从ShuffleMapTasks记录的地图输出的源。
    * 在执行器上,它只是作为一个缓存,其中一个错误触发了从中获取驱动程序对应的HashMap
     * Note: because mapStatuses is accessed concurrently, subclasses should make sure it's a
     * thread-safe map.
    * 注意：由于mapStatuses被并发访问,子类应该确保它是线程安全的映射
   */
  protected val mapStatuses: Map[Int, Array[MapStatus]]

  /**
   * Incremented every time a fetch fails so that client nodes know to clear
   * their cache of map output locations if this happens.
    * 每次提取失败时都会增加,以便客户端节点知道如果发生这种情况,则清除其Map输出位置的缓存
   */
  protected var epoch: Long = 0
  protected val epochLock = new AnyRef//对象引用

  /**
    *  Remembers which map output locations are currently being fetched on an executor.
    *  记住当前正在执行器上获取映射输出的位置。
    * */
  private val fetching = new HashSet[Int]

  /**
   * Send a message to the trackerEndpoint and get its result within a default timeout, or
   * throw a SparkException if this fails.
   * 发一个消息到trackerEndpoint并在规定时间内返回,否则抛出异常
   */
  protected def askTracker[T: ClassTag](message: Any): T = {
    try {
      trackerEndpoint.askWithRetry[T](message)
    } catch {
      case e: Exception =>
        logError("Error communicating with MapOutputTracker", e)
        throw new SparkException("Error communicating with MapOutputTracker", e)
    }
  }

  /** 
   *  Send a one-way message to the trackerEndpoint, to which we expect it to reply with true. 
   *  发一个消息到trackerEndpoint并在规定时间内返回,否则抛出异常,返回true值
   *  */
  protected def sendTracker(message: Any) {
    val response = askTracker[Boolean](message)
    if (response != true) {
      throw new SparkException(
        "Error reply received from MapOutputTracker. Expecting true, got " + response.toString)
    }
  }

  /**
   * getMapSizesByExecutorId 获取Map任务状态
   * 处理步骤如下:
   * 1)从当前BlockManager的MapOutputTracker中获取MapStatuses,若没有就进入2),否则进入第4)步
   * 2)如果获取列表中已经存在要获取的ShuffleId,那么就等待其他线程获取,如果获取列表中不存在要取的shuffleId,
   *   那么就将ShuffleId放入获取列表
   * 3)调用askTracker方法向MapOutputTrackerMaster发送getMapOutputStatuses消息后,将请求的map任务状态信息序列
   *   化后发送给请求方.请求方收到Map任务状态信息后进行反序列化操作,然后放入本地的MapStatuses中
   * 4)调用MapOutputTracker的convertMapStatuses方法,获得MapStatus转换为Map任务所在的地址(BlockManagerId)和
   *    Map任务输出中分配给当前reduce任务的Block大小
   * Called from executors to get the server URIs and output sizes for each shuffle block that
   * needs to be read from a given reduce task.
   *
   * @return A sequence of 2-item tuples, where the first item in the tuple is a BlockManagerId,
   *         and the second item is a sequence of (shuffle block id, shuffle block size) tuples
   *         describing the shuffle blocks that are stored at that block manager.
   */
  def getMapSizesByExecutorId(shuffleId: Int, reduceId: Int)
  : Seq[(BlockManagerId, Seq[(BlockId, Long)])] = {
    logDebug(s"Fetching outputs for shuffle $shuffleId, reduce $reduceId")
    val startTime = System.currentTimeMillis
    //1)从当前BlockManager的MapOutputTracker中获取MapStatuses,若没有就进入2)
    val statuses = mapStatuses.get(shuffleId).orNull
    if (statuses == null) {
      logInfo("Don't have map outputs for shuffle " + shuffleId + ", fetching them")
      var fetchedStatuses: Array[MapStatus] = null
      fetching.synchronized {
        // Someone else is fetching it; wait for them to be done
        //如果获取列表中已经存在要获取的ShuffleId,那么就等待其他线程获取
        while (fetching.contains(shuffleId)) {
          try {
            fetching.wait()
          } catch {
            case e: InterruptedException =>
          }
        }

        // Either while we waited the fetch happened successfully, or
        // someone fetched it in between the get and the fetching.synchronized.
        //果获取列表中不存在要取的shuffleId,那么就将ShuffleId放入获取列表
        fetchedStatuses = mapStatuses.get(shuffleId).orNull
        if (fetchedStatuses == null) {
          // We have to do the fetch, get others to wait for us.
          //我们必须要抓取,让别人等我们
          fetching += shuffleId
        }
      }

      if (fetchedStatuses == null) {
        // We won the race to fetch the output locs; do so
        //我们赢得了获取输出地点的比赛; 这样做
        logInfo("Doing the fetch; tracker endpoint = " + trackerEndpoint)
        // This try-finally prevents hangs due to timeouts:
        //这个try-finally可以防止由于超时引起的挂起：
        try {
          //调用askTracker方法向MapOutputTrackerMaster发送getMapOutputStatuses消息后,
         //将请求的map任务状态信息序列化后,发送给请求方.请求方收到Map任务状态信息后进行反序列化操作,然后放入本地的MapStatuses中
          val fetchedBytes = askTracker[Array[Byte]](GetMapOutputStatuses(shuffleId))
          fetchedStatuses = MapOutputTracker.deserializeMapStatuses(fetchedBytes)
          logInfo("Got the output locations")
          mapStatuses.put(shuffleId, fetchedStatuses)
        } finally {
          fetching.synchronized {
            fetching -= shuffleId
            fetching.notifyAll()
          }
        }
      }
      logDebug(s"Fetching map output location for shuffle $shuffleId, reduce $reduceId took " +
        s"${System.currentTimeMillis - startTime} ms")

      if (fetchedStatuses != null) {
        fetchedStatuses.synchronized {
          return MapOutputTracker.convertMapStatuses(shuffleId, reduceId, fetchedStatuses)
        }
      } else {
        logError("Missing all output locations for shuffle " + shuffleId)
        throw new MetadataFetchFailedException(
          shuffleId, reduceId, "Missing all output locations for shuffle " + shuffleId)
      }
    } else {
      //调用MapOutputTracker的convertMapStatuses方法,
      //获得MapStatus转换为Map任务所在的地址(BlockManagerId)和 Map任务输出中分配给当前reduce任务的Block大小
      statuses.synchronized {
        return MapOutputTracker.convertMapStatuses(shuffleId, reduceId, statuses)
      }
    }
  }

  /** Called to get current epoch(时间上的一点) number. */
  def getEpoch: Long = {
    epochLock.synchronized {
      return epoch
    }
  }

  /**
   * Called from executors to update the epoch number, potentially clearing old outputs
   * 调用executors更新epoch数,可能获取失败清除旧outputs,每个执行任务调用在驱动程序创建时生成最新epoch
   * because of a fetch failure. Each executor task calls this with the latest epoch
   * number on the driver at the time it was created.
   */
  def updateEpoch(newEpoch: Long) {
    epochLock.synchronized {
      if (newEpoch > epoch) {
        logInfo("Updating epoch to " + newEpoch + " and clearing cache")
        epoch = newEpoch
        mapStatuses.clear()
      }
    }
  }

  /** Unregister shuffle data. 取消注册Shuffle数据*/
  def unregisterShuffle(shuffleId: Int) {
    mapStatuses.remove(shuffleId)
  }

  /** Stop the tracker. 停止跟踪器*/
  def stop() { }
}

/**
 * MapOutputTracker for the driver. This uses TimeStampedHashMap to keep track of map
 * output information, which allows old output information based on a TTL.
  * MapOutputTracker为驱动程序,这使用TimeStampedHashMap跟踪地图输出信息,这允许基于TTL的旧输出信息。
 */
private[spark] class MapOutputTrackerMaster(conf: SparkConf)
  extends MapOutputTracker(conf) {

  /**
    * Cache a serialized version of the output statuses for each shuffle to send them out faster
    * 缓存每个随机shuffle的输出状态的序列化版本,以便更快地将它们发送出去
    * */
  private var cacheEpoch = epoch

  /**
   * Timestamp based HashMap for storing mapStatuses and cached serialized statuses in the driver,
   * so that statuses are dropped only by explicit de-registering or by TTL-based cleaning (if set).
   * Other than these two scenarios, nothing should be dropped from this HashMap.
   * 维护跟踪各个Map任务的输出状态,其中Key对应ShuffledId,Array存储各个map任务对应的状态信息MapStatus.
   * 由于MapStatus维护了map输出Block的地址BlockManagerId,所以reduce任务知道从何处获取map任务中间输出.
   */
  protected val mapStatuses = new TimeStampedHashMap[Int, Array[MapStatus]]()
  //维护序列化后的各个map任务输出状态,其中Key对应ShuffleId,Array存储各个序列化MapStatus生成的字节数组
  private val cachedSerializedStatuses = new TimeStampedHashMap[Int, Array[Byte]]()

  // For cleaning up TimeStampedHashMaps
  //清除元数据
  private val metadataCleaner =
    new MetadataCleaner(MetadataCleanerType.MAP_OUTPUT_TRACKER, this.cleanup, conf)  
   //registerMapOutPuts来保存计算结果,这个结果不是真实的数据,而是这些数据的位置,大小等元数据信息,
   //这些下游的task就可以通过这些元数据信息获取其他需要处理的数据
  def registerShuffle(shuffleId: Int, numMaps: Int) {
    //将shuffleId、numMaps大小和MapStatus类型的Array数组的映射关系,放入mapStatuses中  
    //mapStatuses为TimeStampedHashMap[Int, Array[MapStatus]]类型的数据结构  
    if (mapStatuses.put(shuffleId, new Array[MapStatus](numMaps)).isDefined) {
      throw new IllegalArgumentException("Shuffle ID " + shuffleId + " registered twice")
    }
  }
  //注册Map任务
  def registerMapOutput(shuffleId: Int, mapId: Int, status: MapStatus) {
    val array = mapStatuses(shuffleId)
    array.synchronized {//同步并发
      array(mapId) = status
    }
  }

  /** 
   *  Register multiple map output information for the given shuffle 
   *  Shuffle ID为key将MapStatus的列表存入带有时间戳的HashMap
   *  */
  def registerMapOutputs(shuffleId: Int, statuses: Array[MapStatus], changeEpoch: Boolean = false) {
    mapStatuses.put(shuffleId, Array[MapStatus]() ++ statuses)
    if (changeEpoch) {
      incrementEpoch()
    }
  }

  /** Unregister map output information of the given shuffle, mapper and block manager
    * 取消注册给定的shuffle，映射器和块管理器的映射输出信息*/
  def unregisterMapOutput(shuffleId: Int, mapId: Int, bmAddress: BlockManagerId) {
    val arrayOpt = mapStatuses.get(shuffleId)
    if (arrayOpt.isDefined && arrayOpt.get != null) {
      val array = arrayOpt.get
      array.synchronized {
        if (array(mapId) != null && array(mapId).location == bmAddress) {
          array(mapId) = null
        }
      }
      incrementEpoch()
    } else {
      throw new SparkException("unregisterMapOutput called for nonexistent shuffle ID")
    }
  }

  /** Unregister shuffle data 取消注册shuffle放数据*/
  override def unregisterShuffle(shuffleId: Int) {
    mapStatuses.remove(shuffleId)
    cachedSerializedStatuses.remove(shuffleId)
  }

  /**
   *  Check if the given shuffle is being tracked 
	 *  检查给定的shuffleId是否正在被跟踪
   *  */
  def containsShuffle(shuffleId: Int): Boolean = {
    cachedSerializedStatuses.contains(shuffleId) || mapStatuses.contains(shuffleId)
  }

  /**
   * Return a list of locations that each have fraction of map output greater than the specified
   * threshold.
   * 返回一个位置列表,每个位置都有大于指定阈值的Map输出。
   * @param shuffleId id of the shuffle
   * @param reducerId id of the reduce task
   * @param numReducers total number of reducers in the shuffle
   * @param fractionThreshold fraction of total map output size that a location must have
   *                          for it to be considered large.
    *                         一个位置必须具有的总映射输出大小的分数被认为是大的
   *
   * This method is not thread-safe. 这种方法不是线程安全的
   */
  def getLocationsWithLargestOutputs(
      shuffleId: Int,
      reducerId: Int,
      numReducers: Int,
      fractionThreshold: Double)
    : Option[Array[BlockManagerId]] = {

    if (mapStatuses.contains(shuffleId)) {
      val statuses = mapStatuses(shuffleId)
      if (statuses.nonEmpty) {
        // HashMap to add up sizes of all blocks at the same location
        //HashMap将相同位置的所有块的大小相加
        val locs = new HashMap[BlockManagerId, Long]
        var totalOutputSize = 0L
        var mapIdx = 0
        while (mapIdx < statuses.length) {
          val status = statuses(mapIdx)
          val blockSize = status.getSizeForBlock(reducerId)
          if (blockSize > 0) {
            locs(status.location) = locs.getOrElse(status.location, 0L) + blockSize
            totalOutputSize += blockSize
          }
          mapIdx = mapIdx + 1
        }
        val topLocs = locs.filter { case (loc, size) =>
          size.toDouble / totalOutputSize >= fractionThreshold
        }
        // Return if we have any locations which satisfy the required threshold
        //如果我们有任何满足所需阈值的位置返回
        if (topLocs.nonEmpty) {
          return Some(topLocs.map(_._1).toArray)
        }
      }
    }
    None
  }

  def incrementEpoch() {
    epochLock.synchronized {
      epoch += 1
      logDebug("Increasing epoch to " + epoch)
    }
  }

  def getSerializedMapOutputStatuses(shuffleId: Int): Array[Byte] = {
    var statuses: Array[MapStatus] = null
    var epochGotten: Long = -1
    epochLock.synchronized {//对象引用同步
      if (epoch > cacheEpoch) {
        cachedSerializedStatuses.clear()
        cacheEpoch = epoch
      }
      //维护序列化后的各个map任务输出状态,其中Key对应ShuffleId,Array存储各个序列化MapStatus生成的字节数组
      cachedSerializedStatuses.get(shuffleId) match {
        case Some(bytes) =>
          return bytes
        case None =>
          statuses = mapStatuses.getOrElse(shuffleId, Array[MapStatus]())
          epochGotten = epoch
      }
    }
    // If we got here, we failed to find the serialized locations in the cache, so we pulled
    // out a snapshot of the locations as "statuses"; let's serialize and return that
    //如果我们到达这里，我们没有在缓存中找到序列化的位置，所以我们将这些位置的快照提取为“状态”; 让我们序列化并返回
    val bytes = MapOutputTracker.serializeMapStatuses(statuses)
    logInfo("Size of output statuses for shuffle %d is %d bytes".format(shuffleId, bytes.length))
    // Add them into the table only if the epoch hasn't changed while we were working
    //将它们添加到表中,只有当我们正在工作的时代没有改变
    epochLock.synchronized {
      if (epoch == epochGotten) {
        cachedSerializedStatuses(shuffleId) = bytes
      }
    }
    bytes
  }

  override def stop() {
    sendTracker(StopMapOutputTracker)
    mapStatuses.clear()
    trackerEndpoint = null
    metadataCleaner.cancel()
    cachedSerializedStatuses.clear()
  }

  private def cleanup(cleanupTime: Long) {
    mapStatuses.clearOldValues(cleanupTime)
    cachedSerializedStatuses.clearOldValues(cleanupTime)
  }
}

/**
 * MapOutputTracker for the executors, which fetches map output information from the driver's
 * MapOutputTrackerMaster.
  * MapOutputTracker用于执行器,它从驱动程序的MapOutputTrackerMaster中获取映射输出信息。
 */
private[spark] class MapOutputTrackerWorker(conf: SparkConf) extends MapOutputTracker(conf) {
  protected val mapStatuses: Map[Int, Array[MapStatus]] =
    new ConcurrentHashMap[Int, Array[MapStatus]]
}

private[spark] object MapOutputTracker extends Logging {

  val ENDPOINT_NAME = "MapOutputTracker"

  // Serialize an array of map output locations into an efficient byte format so that we can send
  // it to reduce tasks. We do this by compressing the serialized bytes using GZIP. They will
  // generally be pretty compressible because many map outputs will be on the same hostname.
  //将地图输出位置的数组序列化为高效的字节格式,以便我们可以将其发送以减少任务。
  //我们通过使用GZIP压缩序列化字节来实现。 它们通常是非常可压缩的,因为很多地图输出将在同一个主机名上。
  def serializeMapStatuses(statuses: Array[MapStatus]): Array[Byte] = {
    val out = new ByteArrayOutputStream
    val objOut = new ObjectOutputStream(new GZIPOutputStream(out))
    //柯里化函数
    Utils.tryWithSafeFinally {
      // Since statuses can be modified in parallel, sync on it
      //同步并发修改数据
      statuses.synchronized {
        objOut.writeObject(statuses)
      }
    } {
      objOut.close()
    }
    out.toByteArray
  }

  // Opposite of serializeMapStatuses.
  def deserializeMapStatuses(bytes: Array[Byte]): Array[MapStatus] = {
    val objIn = new ObjectInputStream(new GZIPInputStream(new ByteArrayInputStream(bytes)))
    Utils.tryWithSafeFinally {
      objIn.readObject().asInstanceOf[Array[MapStatus]]
    } {
      objIn.close()
    }
  }

  /**
   * Converts an array of MapStatuses for a given reduce ID to a sequence that, for each block
   * manager ID, lists the shuffle block ids and corresponding shuffle block sizes stored at that
   * block manager.
    * 将给定的减少ID的MapStatus数组转换为每个块管理器ID列出存储在该块管理器中的随机块ID和相应的随机块大小的序列。
   *
   * If any of the statuses is null (indicating a missing location due to a failed mapper),
   * throws a FetchFailedException.
    * 如果任何状态为空(指示由于失败的映射器而导致的缺失位置),则抛出FetchFailedException。
   *	map任务地址转换
   * @param shuffleId Identifier for the shuffle
   * @param reduceId Identifier for the reduce task
   * @param statuses List of map statuses, indexed by map ID.
   * @return A sequence of 2-item tuples, where the first item in the tuple is a BlockManagerId,
   *         and the second item is a sequence of (shuffle block id, shuffle block size) tuples
   *         describing the shuffle blocks that are stored at that block manager.
    *         一个2项元组的序列，其中元组中的第一个项是BlockManagerId，第二个项目是(shuffle block id，shuffle block size)
    *         元组的序列描述存储在该块管理器中的随机块。
   */
  private def convertMapStatuses(
      shuffleId: Int,
      reduceId: Int,
      statuses: Array[MapStatus]): Seq[(BlockManagerId, Seq[(BlockId, Long)])] = {
    assert (statuses != null)
    val splitsByAddress = new HashMap[BlockManagerId, ArrayBuffer[(BlockId, Long)]]
    for ((status, mapId) <- statuses.zipWithIndex) {
      if (status == null) {
        val errorMessage = s"Missing an output location for shuffle $shuffleId"
        logError(errorMessage)
        throw new MetadataFetchFailedException(shuffleId, reduceId, errorMessage)
      } else {
        splitsByAddress.getOrElseUpdate(status.location, ArrayBuffer()) +=
          ((ShuffleBlockId(shuffleId, mapId, reduceId), status.getSizeForBlock(reduceId)))
      }
    }

    splitsByAddress.toSeq
  }
}
