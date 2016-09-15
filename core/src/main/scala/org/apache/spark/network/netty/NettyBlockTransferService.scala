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

package org.apache.spark.network.netty

import scala.collection.JavaConversions._
import scala.concurrent.{ Future, Promise }

import org.apache.spark.{ SecurityManager, SparkConf }
import org.apache.spark.network._
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.client.{ TransportClientBootstrap, RpcResponseCallback, TransportClientFactory }
import org.apache.spark.network.sasl.{ SaslClientBootstrap, SaslServerBootstrap }
import org.apache.spark.network.server._
import org.apache.spark.network.shuffle.{ RetryingBlockFetcher, BlockFetchingListener, OneForOneBlockFetcher }
import org.apache.spark.network.shuffle.protocol.UploadBlock
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.storage.{ BlockId, StorageLevel }
import org.apache.spark.util.Utils

/**
 * A BlockTransferService that uses Netty to fetch a set of blocks at at time.
 * 块传输服务,使用Netty可以异步事件驱动的网络应用框架,提供Web服务及客户端,获取远程节点上Block的集合
 * 为什么提供Shuffle服务与客户端
 * Spark是分布式部署的,每个task最终都运行在不同的机器节点上,map任务的输出结果直接存储到map任务所在机器的存储体系中
 * reduce任务极有可能不在同一台机器上运行,所以需要远程下载map任务的中间结果输出
 *
 * BlockTransferServicer提供shuufle文件上传到其他Executor或者下载到本地的客户端,
 * 也提供了可以被其他Executor访问的Shuufle服务
 */
class NettyBlockTransferService(conf: SparkConf, securityManager: SecurityManager, numCores: Int)
    extends BlockTransferService {

  // TODO: Don't use Java serialization, use a more cross-version compatible serialization format.
  private val serializer = new JavaSerializer(conf)
  private val authEnabled = securityManager.isAuthenticationEnabled()
  private val transportConf = SparkTransportConf.fromSparkConf(conf, numCores)

  private[this] var transportContext: TransportContext = _
  private[this] var server: TransportServer = _
  private[this] var clientFactory: TransportClientFactory = _
  private[this] var appId: String = _
  /**
   * BlockTransferService只有在其init方法被调用,即初始化后才提供服务
   */
  override def init(blockDataManager: BlockDataManager): Unit = {
    //创建RpcServer
    val rpcHandler = new NettyBlockRpcServer(serializer, blockDataManager)

    var serverBootstrap: Option[TransportServerBootstrap] = None
    var clientBootstrap: Option[TransportClientBootstrap] = None
    if (authEnabled) {
      serverBootstrap = Some(new SaslServerBootstrap(transportConf, securityManager))
      clientBootstrap = Some(new SaslClientBootstrap(transportConf, conf.getAppId, securityManager,
        securityManager.isSaslEncryptionEnabled()))
    }
    //构造TransportContext
    transportContext = new TransportContext(transportConf, rpcHandler)
    //创建RPC客户端工厂createClientFactory
    clientFactory = transportContext.createClientFactory(clientBootstrap.toList)
    //创建Netty服务器TransportServer,可以修改属于server.getPort
    server = createServer(serverBootstrap.toList)
    appId = conf.getAppId
    logInfo("Server created on " + server.getPort)
  }

  /**
   *  Creates and binds the TransportServer, possibly trying multiple ports. 
   *  创建和绑定transportserver，可能在多个端口
   *  */
  private def createServer(bootstraps: List[TransportServerBootstrap]): TransportServer = {
    def startService(port: Int): (TransportServer, Int) = {
      val server = transportContext.createServer(port, bootstraps)
      (server, server.getPort)
    }

    val portToTry = conf.getInt("spark.blockManager.port", 0)
    Utils.startServiceOnPort(portToTry, startService, conf, getClass.getName)._1
  }
  //方法用于获取远程shuffle文件,实际是利用NettyBlockTransferService中创建服务
  override def fetchBlocks(
    host: String,
    port: Int,
    execId: String,
    blockIds: Array[String],
    listener: BlockFetchingListener): Unit = {
    logTrace(s"Fetch blocks from $host:$port (executor id $execId)")
    try {
      val blockFetchStarter = new RetryingBlockFetcher.BlockFetchStarter {
        override def createAndStart(blockIds: Array[String], listener: BlockFetchingListener) {
          val client = clientFactory.createClient(host, port)
          new OneForOneBlockFetcher(client, appId, execId, blockIds.toArray, listener).start()
        }
      }
     //最大重试次数
      val maxRetries = transportConf.maxIORetries()
      if (maxRetries > 0) {
        // Note this Fetcher will correctly handle maxRetries == 0; we avoid it just in case there's
        // a bug in this code. We should remove the if statement once we're sure of the stability.
        new RetryingBlockFetcher(transportConf, blockFetchStarter, blockIds, listener).start()
      } else {
        blockFetchStarter.createAndStart(blockIds, listener)
      }
    } catch {
      case e: Exception =>
        logError("Exception while beginning fetchBlocks", e)
        blockIds.foreach(listener.onBlockFetchFailure(_, e))
    }
  }

  override def hostName: String = Utils.localHostName()

  override def port: Int = server.getPort
  //上传shuffle文件到远程Executor,也是利用NettyBlockTransferService中创建服务
  override def uploadBlock(
    hostname: String,
    port: Int,
    execId: String,
    blockId: BlockId,
    blockData: ManagedBuffer,
    level: StorageLevel): Future[Unit] = {
    val result = Promise[Unit]()
    //创建Netty服务的客户端,客户端连接的hostname和port正是我们随机选择的BlockManager的hostname和port
    val client = clientFactory.createClient(hostname, port)

    // StorageLevel is serialized as bytes using our JavaSerializer. Everything else is encoded
    // using our binary protocol.
    //将Block的存储级别StorageLevel序列化
    val levelBytes = serializer.newInstance().serialize(level).array()

    // Convert or copy nio buffer into array in order to serialize it.
    //将Block的ByteBuffer转化数组,便于序列化
    val nioBuffer = blockData.nioByteBuffer()

    val array = if (nioBuffer.hasArray) {
      nioBuffer.array()
    } else {
      val data = new Array[Byte](nioBuffer.remaining())
      nioBuffer.get(data)
      data
    }
    //将appId,execId,blockId,序列化的StorageLevel,转化为数组的Block封装为UploadBlock,
    //并将UploadBlock序列化为数组,调用客户端sendRpc方法将字节数组上传
    client.sendRpc(new UploadBlock(appId, execId, blockId.toString, levelBytes, array).toByteArray,
      //最终调用Netty客户端的SendRpc方法将字节数组上传,回调函数RpcResponseCallback根据RPC的结果更改上传状态
      new RpcResponseCallback {
        override def onSuccess(response: Array[Byte]): Unit = {
          logTrace(s"Successfully uploaded block $blockId")
          result.success()
        }
        override def onFailure(e: Throwable): Unit = {
          logError(s"Error while uploading block $blockId", e)
          result.failure(e)
        }
      })

    result.future
  }

  override def close(): Unit = {
    server.close()
    clientFactory.close()
  }
}
