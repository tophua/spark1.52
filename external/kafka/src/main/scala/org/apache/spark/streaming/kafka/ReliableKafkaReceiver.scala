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

package org.apache.spark.streaming.kafka

import java.util.Properties
import java.util.concurrent.{ThreadPoolExecutor, ConcurrentHashMap}

import scala.collection.{Map, mutable}
import scala.reflect.{ClassTag, classTag}

import kafka.common.TopicAndPartition
import kafka.consumer.{Consumer, ConsumerConfig, ConsumerConnector, KafkaStream}
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import kafka.utils.{VerifiableProperties, ZKGroupTopicDirs, ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient

import org.apache.spark.{Logging, SparkEnv}
import org.apache.spark.storage.{StorageLevel, StreamBlockId}
import org.apache.spark.streaming.receiver.{BlockGenerator, BlockGeneratorListener, Receiver}
import org.apache.spark.util.ThreadUtils

/**
 * ReliableKafkaReceiver offers the ability to reliably store data into BlockManager without loss.
  * ReliableKafkaReceiver提供将数据可靠地存储到BlockManager而不会丢失的能力
 * It is turned off by default and will be enabled when
 * spark.streaming.receiver.writeAheadLog.enable is true. The difference compared to KafkaReceiver
 * is that this receiver manages topic-partition/offset itself and updates the offset information
 * after data is reliably stored as write-ahead log. Offsets will only be updated when data is
 * reliably stored, so the potential data loss problem of KafkaReceiver can be eliminated.
  *
  * 默认情况下关闭,并在spark.streaming.receiver.writeAheadLog.enable为true时启用,与KafkaReceiver相比的差异在于,
  * 该接收器管理主题分区/偏移本身,并且在数据可靠地存储为预写日志之后更新偏移信息,当数据可靠存储时,只有更新偏移量,
  * 才能消除KafkaReceiver潜在的数据丢失问题。
 *
 * Note: ReliableKafkaReceiver will set auto.commit.enable to false to turn off automatic offset
 * commit mechanism in Kafka consumer. So setting this configuration manually within kafkaParams
 * will not take effect.
  * 注意：ReliableKafkaReceiver将auto.commit.enable设置为false,以关闭Kafka消费者中的自动偏移提交机制。
  * 因此,在kafkaParams中手动设置此配置将不会生效
 */
private[streaming]
class ReliableKafkaReceiver[
  K: ClassTag,
  V: ClassTag,
  U <: Decoder[_]: ClassTag,
  T <: Decoder[_]: ClassTag](
    kafkaParams: Map[String, String],
    topics: Map[String, Int],
    storageLevel: StorageLevel)
    extends Receiver[(K, V)](storageLevel) with Logging {

  private val groupId = kafkaParams("group.id")
  private val AUTO_OFFSET_COMMIT = "auto.commit.enable"
  private def conf = SparkEnv.get.conf

  /** High level consumer to connect to Kafka.
    * 高级消费者连接到卡夫卡 */
  private var consumerConnector: ConsumerConnector = null

  /** zkClient to connect to Zookeeper to commit the offsets.
    * zkClient连接到Zookeeper来提交偏移量 */
  private var zkClient: ZkClient = null

  /**
   * A HashMap to manage the offset for each topic/partition, this HashMap is called in
   * synchronized block, so mutable HashMap will not meet concurrency issue.
    * 一个HashMap来管理每个主题/分区的偏移量,这个HashMap在同步块中被调用,所以可变的HashMap将不会遇到并发问题
   */
  private var topicPartitionOffsetMap: mutable.HashMap[TopicAndPartition, Long] = null

  /** A concurrent HashMap to store the stream block id and related offset snapshot.
    * 并发HashMap来存储流块ID和相关的偏移快照*/
  private var blockOffsetMap: ConcurrentHashMap[StreamBlockId, Map[TopicAndPartition, Long]] = null

  /**
   * Manage the BlockGenerator in receiver itself for better managing block store and offset
   * commit.
    * 在接收器本身管理BlockGenerator以更好地管理块存储和偏移提交
   */
  private var blockGenerator: BlockGenerator = null

  /** Thread pool running the handlers for receiving message from multiple topics and partitions.
    * 线程池运行处理程序,用于从多个主题和分区接收消息 */
  private var messageHandlerThreadPool: ThreadPoolExecutor = null

  override def onStart(): Unit = {
    logInfo(s"Starting Kafka Consumer Stream with group: $groupId")

    // Initialize the topic-partition / offset hash map.
    //初始化主题 - 分区/偏移哈希映射
    topicPartitionOffsetMap = new mutable.HashMap[TopicAndPartition, Long]

    // Initialize the stream block id / offset snapshot hash map.
    //初始化流块id /偏移快照哈希映射。
    blockOffsetMap = new ConcurrentHashMap[StreamBlockId, Map[TopicAndPartition, Long]]()

    // Initialize the block generator for storing Kafka message.
    //初始化块生成器以存储Kafka消息
    blockGenerator = supervisor.createBlockGenerator(new GeneratedBlockHandler)

    if (kafkaParams.contains(AUTO_OFFSET_COMMIT) && kafkaParams(AUTO_OFFSET_COMMIT) == "true") {
      logWarning(s"$AUTO_OFFSET_COMMIT should be set to false in ReliableKafkaReceiver, " +
        "otherwise we will manually set it to false to turn off auto offset commit in Kafka")
    }

    val props = new Properties()
    kafkaParams.foreach(param => props.put(param._1, param._2))
    // Manually set "auto.commit.enable" to "false" no matter user explicitly set it to true,
    //手动将“auto.commit.enable”设置为“false”,无论用户将其设置为true
    // we have to make sure this property is set to false to turn off auto commit mechanism in
    // Kafka.
    //我们必须确保将此属性设置为false以关闭Kafka中的自动提交机制
    props.setProperty(AUTO_OFFSET_COMMIT, "false")

    val consumerConfig = new ConsumerConfig(props)

    assert(!consumerConfig.autoCommitEnable)

    logInfo(s"Connecting to Zookeeper: ${consumerConfig.zkConnect}")
    consumerConnector = Consumer.create(consumerConfig)
    logInfo(s"Connected to Zookeeper: ${consumerConfig.zkConnect}")

    zkClient = new ZkClient(consumerConfig.zkConnect, consumerConfig.zkSessionTimeoutMs,
      consumerConfig.zkConnectionTimeoutMs, ZKStringSerializer)

    messageHandlerThreadPool = ThreadUtils.newDaemonFixedThreadPool(
      topics.values.sum, "KafkaMessageHandler")

    blockGenerator.start()

    val keyDecoder = classTag[U].runtimeClass.getConstructor(classOf[VerifiableProperties])
      .newInstance(consumerConfig.props)
      .asInstanceOf[Decoder[K]]

    val valueDecoder = classTag[T].runtimeClass.getConstructor(classOf[VerifiableProperties])
      .newInstance(consumerConfig.props)
      .asInstanceOf[Decoder[V]]

    val topicMessageStreams = consumerConnector.createMessageStreams(
      topics, keyDecoder, valueDecoder)

    topicMessageStreams.values.foreach { streams =>
      streams.foreach { stream =>
        messageHandlerThreadPool.submit(new MessageHandler(stream))
      }
    }
  }

  override def onStop(): Unit = {
    if (messageHandlerThreadPool != null) {
      messageHandlerThreadPool.shutdown()
      messageHandlerThreadPool = null
    }

    if (consumerConnector != null) {
      consumerConnector.shutdown()
      consumerConnector = null
    }

    if (zkClient != null) {
      zkClient.close()
      zkClient = null
    }

    if (blockGenerator != null) {
      blockGenerator.stop()
      blockGenerator = null
    }

    if (topicPartitionOffsetMap != null) {
      topicPartitionOffsetMap.clear()
      topicPartitionOffsetMap = null
    }

    if (blockOffsetMap != null) {
      blockOffsetMap.clear()
      blockOffsetMap = null
    }
  }

  /** Store a Kafka message and the associated metadata as a tuple.
    * 将Kafka消息和关联的元数据存储为元组
    * */
  private def storeMessageAndMetadata(
      msgAndMetadata: MessageAndMetadata[K, V]): Unit = {
    val topicAndPartition = TopicAndPartition(msgAndMetadata.topic, msgAndMetadata.partition)
    val data = (msgAndMetadata.key, msgAndMetadata.message)
    val metadata = (topicAndPartition, msgAndMetadata.offset)
    blockGenerator.addDataWithCallback(data, metadata)
  }

  /** Update stored offset
    * 更新存储的偏移*/
  private def updateOffset(topicAndPartition: TopicAndPartition, offset: Long): Unit = {
    topicPartitionOffsetMap.put(topicAndPartition, offset)
  }

  /**
   * Remember the current offsets for each topic and partition. This is called when a block is
   * generated.
    * 记住每个主题和分区的当前偏移量,当生成块时调用它
   */
  private def rememberBlockOffsets(blockId: StreamBlockId): Unit = {
    // Get a snapshot of current offset map and store with related block id.
    //获取当前偏移量映射的快照,并存储相关的块ID
    val offsetSnapshot = topicPartitionOffsetMap.toMap
    blockOffsetMap.put(blockId, offsetSnapshot)
    topicPartitionOffsetMap.clear()
  }

  /**
   * Store the ready-to-be-stored block and commit the related offsets to zookeeper. This method
   * will try a fixed number of times to push the block. If the push fails, the receiver is stopped.
    * 存储即将存储的块,并将相关的偏移量提交给zookeeper,该方法将尝试固定次数来推动块,如果推送失败,接收机将停止
   */
  private def storeBlockAndCommitOffset(
      blockId: StreamBlockId, arrayBuffer: mutable.ArrayBuffer[_]): Unit = {
    var count = 0
    var pushed = false
    var exception: Exception = null
    while (!pushed && count <= 3) {
      try {
        store(arrayBuffer.asInstanceOf[mutable.ArrayBuffer[(K, V)]])
        pushed = true
      } catch {
        case ex: Exception =>
          count += 1
          exception = ex
      }
    }
    if (pushed) {
      Option(blockOffsetMap.get(blockId)).foreach(commitOffset)
      blockOffsetMap.remove(blockId)
    } else {
      stop("Error while storing block into Spark", exception)
    }
  }

  /**
   * Commit the offset of Kafka's topic/partition, the commit mechanism follow Kafka 0.8.x's
   * metadata schema in Zookeeper.
    * 提交Kafka主题/分区的偏移,提交机制遵循Zookeeper中的Kafka 0.8.x的元数据模式
   */
  private def commitOffset(offsetMap: Map[TopicAndPartition, Long]): Unit = {
    if (zkClient == null) {
      val thrown = new IllegalStateException("Zookeeper client is unexpectedly null")
      stop("Zookeeper client is not initialized before commit offsets to ZK", thrown)
      return
    }

    for ((topicAndPart, offset) <- offsetMap) {
      try {
        val topicDirs = new ZKGroupTopicDirs(groupId, topicAndPart.topic)
        val zkPath = s"${topicDirs.consumerOffsetDir}/${topicAndPart.partition}"

        ZkUtils.updatePersistentPath(zkClient, zkPath, offset.toString)
      } catch {
        case e: Exception =>
          logWarning(s"Exception during commit offset $offset for topic" +
            s"${topicAndPart.topic}, partition ${topicAndPart.partition}", e)
      }

      logInfo(s"Committed offset $offset for topic ${topicAndPart.topic}, " +
        s"partition ${topicAndPart.partition}")
    }
  }

  /** Class to handle received Kafka message.
    * 接收Kafka消息的类 */
  private final class MessageHandler(stream: KafkaStream[K, V]) extends Runnable {
    override def run(): Unit = {
      while (!isStopped) {
        try {
          val streamIterator = stream.iterator()
          while (streamIterator.hasNext) {
            storeMessageAndMetadata(streamIterator.next)
          }
        } catch {
          case e: Exception =>
            reportError("Error handling message", e)
        }
      }
    }
  }

  /** Class to handle blocks generated by the block generator.
    * 用于处理块生成器生成的块的类*/
  private final class GeneratedBlockHandler extends BlockGeneratorListener {

    def onAddData(data: Any, metadata: Any): Unit = {
      // Update the offset of the data that was added to the generator
      //更新添加到生成器的数据的偏移量
      if (metadata != null) {
        val (topicAndPartition, offset) = metadata.asInstanceOf[(TopicAndPartition, Long)]
        updateOffset(topicAndPartition, offset)
      }
    }

    def onGenerateBlock(blockId: StreamBlockId): Unit = {
      // Remember the offsets of topics/partitions when a block has been generated
      //记住生成块时主题/分区的偏移量
      rememberBlockOffsets(blockId)
    }

    def onPushBlock(blockId: StreamBlockId, arrayBuffer: mutable.ArrayBuffer[_]): Unit = {
      // Store block and commit the blocks offset
      //存储块并提交块偏移量
      storeBlockAndCommitOffset(blockId, arrayBuffer)
    }

    def onError(message: String, throwable: Throwable): Unit = {
      reportError(message, throwable)
    }
  }
}
