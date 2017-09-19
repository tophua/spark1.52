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

import java.io.File

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.concurrent.Eventually

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.util.Utils

class ReliableKafkaStreamSuite extends SparkFunSuite
    with BeforeAndAfterAll with BeforeAndAfter with Eventually {

  private val sparkConf = new SparkConf()
    .setMaster("local[4]")
    .setAppName(this.getClass.getSimpleName)
    .set("spark.streaming.receiver.writeAheadLog.enable", "true")
  private val data = Map("a" -> 10, "b" -> 10, "c" -> 10)

  private var kafkaTestUtils: KafkaTestUtils = _

  private var groupId: String = _
  private var kafkaParams: Map[String, String] = _
  private var ssc: StreamingContext = _
  private var tempDirectory: File = null

  override def beforeAll() : Unit = {
    kafkaTestUtils = new KafkaTestUtils
    kafkaTestUtils.setup()

    groupId = s"test-consumer-${Random.nextInt(10000)}"
    kafkaParams = Map(
      "zookeeper.connect" -> kafkaTestUtils.zkAddress,
      "group.id" -> groupId,
      "auto.offset.reset" -> "smallest"
    )

    tempDirectory = Utils.createTempDir()
  }

  override def afterAll(): Unit = {
    Utils.deleteRecursively(tempDirectory)

    if (kafkaTestUtils != null) {
      kafkaTestUtils.teardown()
      kafkaTestUtils = null
    }
  }

  before {
    ssc = new StreamingContext(sparkConf, Milliseconds(500))
    ssc.checkpoint(tempDirectory.getAbsolutePath)
  }

  after {
    if (ssc != null) {
      ssc.stop()
      ssc = null
    }
  }
  //可靠的Kafka输入流有单个主题
  test("Reliable Kafka input stream with single topic") {
    val topic = "test-topic"
    kafkaTestUtils.createTopic(topic)
    kafkaTestUtils.sendMessages(topic, data)

    // Verify whether the offset of this group/topic/partition is 0 before starting.
    //验证此组/主题/分区的偏移量是否在开始前为0
    assert(getCommitOffset(groupId, topic, 0) === None)

    val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Map(topic -> 1), StorageLevel.MEMORY_ONLY)
    val result = new mutable.HashMap[String, Long]()
    stream.map { case (k, v) => v }.foreachRDD { r =>
        val ret = r.collect()
        ret.foreach { v =>
          val count = result.getOrElseUpdate(v, 0) + 1
          result.put(v, count)
        }
      }
    ssc.start()

    eventually(timeout(20000 milliseconds), interval(200 milliseconds)) {
      // A basic process verification for ReliableKafkaReceiver.
      //一个reliablekafkareceiver基本过程验证
      // Verify whether received message number is equal to the sent message number.
      //验证接收的消息数是否等于所发送的消息数
      assert(data.size === result.size)
      // Verify whether each message is the same as the data to be verified.
      //验证每个消息是否是相同的作为验证的数据
      data.keys.foreach { k => assert(data(k) === result(k).toInt) }
      // Verify the offset number whether it is equal to the total message number.
      //验证偏移数是否等于总消息数
      assert(getCommitOffset(groupId, topic, 0) === Some(29L))
    }
  }
  //可靠的Kafka输入流有多个主题
  test("Reliable Kafka input stream with multiple topics") {
    val topics = Map("topic1" -> 1, "topic2" -> 1, "topic3" -> 1)
    topics.foreach { case (t, _) =>
      kafkaTestUtils.createTopic(t)
      kafkaTestUtils.sendMessages(t, data)
    }

    // Before started, verify all the group/topic/partition offsets are 0.
    //在开始之前,验证所有的组/主题/分区偏移量为0
    topics.foreach { case (t, _) => assert(getCommitOffset(groupId, t, 0) === None) }

    // Consuming all the data sent to the broker which will potential commit the offsets internally.
    //消耗所有发送给代理的数据,这些数据将潜在的提交内部偏移
    val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics, StorageLevel.MEMORY_ONLY)
    stream.foreachRDD(_ => Unit)
    ssc.start()

    eventually(timeout(20000 milliseconds), interval(100 milliseconds)) {
      // Verify the offset for each group/topic to see whether they are equal to the expected one.
      //验证每个组/主题的偏移量,看看它们是否等于预期的一个
      topics.foreach { case (t, _) => assert(getCommitOffset(groupId, t, 0) === Some(29L)) }
    }
  }


  /** 
   *  Getting partition offset from Zookeeper. 
   *  从zookeeper偏移获得分区
   *  */
  private def getCommitOffset(groupId: String, topic: String, partition: Int): Option[Long] = {
    val topicDirs = new ZKGroupTopicDirs(groupId, topic)
    val zkPath = s"${topicDirs.consumerOffsetDir}/$partition"
    ZkUtils.readDataMaybeNull(kafkaTestUtils.zookeeperClient, zkPath)._1.map(_.toLong)
  }
}
