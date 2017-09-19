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

import scala.collection.Map
import scala.reflect.{classTag, ClassTag}

import kafka.consumer.{KafkaStream, Consumer, ConsumerConfig, ConsumerConnector}
import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties

import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.util.ThreadUtils

/**
 * Input stream that pulls messages from a Kafka Broker.
  * 输入流从卡夫卡Broker提取消息
 *
 * @param kafkaParams Map of kafka configuration parameters. kafka配置参数映射
 *                    See: http://kafka.apache.org/configuration.html
 * @param topics Map of (topic_name -> numPartitions) to consume. Each partition is consumed
 * in its own thread.(topic_name - > numPartitions)的映射消费,每个分区都在自己的线程中使用
 * @param storageLevel RDD storage level. RDD存储级别
 */
private[streaming]
class KafkaInputDStream[
  K: ClassTag,
  V: ClassTag,
  U <: Decoder[_]: ClassTag,
  T <: Decoder[_]: ClassTag](
    @transient ssc_ : StreamingContext,
    kafkaParams: Map[String, String],
    topics: Map[String, Int],
    useReliableReceiver: Boolean,
    storageLevel: StorageLevel
  ) extends ReceiverInputDStream[(K, V)](ssc_) with Logging {

  def getReceiver(): Receiver[(K, V)] = {
    if (!useReliableReceiver) {
      new KafkaReceiver[K, V, U, T](kafkaParams, topics, storageLevel)
    } else {
      new ReliableKafkaReceiver[K, V, U, T](kafkaParams, topics, storageLevel)
    }
  }
}

private[streaming]
class KafkaReceiver[
  K: ClassTag,
  V: ClassTag,
  U <: Decoder[_]: ClassTag,
  T <: Decoder[_]: ClassTag](
    kafkaParams: Map[String, String],
    topics: Map[String, Int],
    storageLevel: StorageLevel
  ) extends Receiver[(K, V)](storageLevel) with Logging {

  // Connection to Kafka 连接到卡夫卡
  var consumerConnector: ConsumerConnector = null

  def onStop() {
    if (consumerConnector != null) {
      consumerConnector.shutdown()
      consumerConnector = null
    }
  }

  def onStart() {

    logInfo("Starting Kafka Consumer Stream with group: " + kafkaParams("group.id"))

    // Kafka connection properties
    //卡夫卡连接属性
    val props = new Properties()
    kafkaParams.foreach(param => props.put(param._1, param._2))

    val zkConnect = kafkaParams("zookeeper.connect")
    // Create the connection to the cluster
    //创建与集群的连接
    logInfo("Connecting to Zookeeper: " + zkConnect)
    val consumerConfig = new ConsumerConfig(props)
    consumerConnector = Consumer.create(consumerConfig)
    logInfo("Connected to " + zkConnect)

    val keyDecoder = classTag[U].runtimeClass.getConstructor(classOf[VerifiableProperties])
      .newInstance(consumerConfig.props)
      .asInstanceOf[Decoder[K]]
    val valueDecoder = classTag[T].runtimeClass.getConstructor(classOf[VerifiableProperties])
      .newInstance(consumerConfig.props)
      .asInstanceOf[Decoder[V]]

    // Create threads for each topic/message Stream we are listening
    //为每个主题/消息创建线程我们正在监听
    val topicMessageStreams = consumerConnector.createMessageStreams(
      topics, keyDecoder, valueDecoder)

    val executorPool =
      ThreadUtils.newDaemonFixedThreadPool(topics.values.sum, "KafkaMessageHandler")
    try {
      // Start the messages handler for each partition
      //启动每个分区的消息处理程序
      topicMessageStreams.values.foreach { streams =>
        streams.foreach { stream => executorPool.submit(new MessageHandler(stream)) }
      }
    } finally {
      //在工作完成后，只需让线程终止
      executorPool.shutdown() // Just causes threads to terminate after work is done
    }
  }

  // Handles Kafka messages
  //处理卡夫卡消息
  private class MessageHandler(stream: KafkaStream[K, V])
    extends Runnable {
    def run() {
      logInfo("Starting MessageHandler.")
      try {
        val streamIterator = stream.iterator()
        while (streamIterator.hasNext()) {
          val msgAndMetadata = streamIterator.next()
          store((msgAndMetadata.key, msgAndMetadata.message))
        }
      } catch {
        case e: Throwable => reportError("Error handling message; exiting", e)
      }
    }
  }
}
