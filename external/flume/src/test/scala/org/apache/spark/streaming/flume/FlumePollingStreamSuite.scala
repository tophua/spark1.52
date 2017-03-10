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

package org.apache.spark.streaming.flume

import java.net.InetSocketAddress

import scala.collection.JavaConversions._
import scala.collection.mutable.{SynchronizedBuffer, ArrayBuffer}
import scala.concurrent.duration._
import scala.language.postfixOps

import com.google.common.base.Charsets.UTF_8
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually._

import org.apache.spark.{Logging, SparkConf, SparkFunSuite}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, TestOutputStream, StreamingContext}
import org.apache.spark.util.{ManualClock, Utils}
/**
 * flume的核心是把数据从数据源收集过来,再送到目的地。
 * 为了保证输送一定成功,在送到目的地之前,会先缓存数据,待数据真正到达目的地后,删除自己缓存的数据。
 * source可以接收外部源发送过来的数据,不同的source,可以接受不同的数据格式。
 * channel是一个存储地,接收source的输出,直到有sink消费掉channel中的数据。
 * sink组件会消费channel中的数据,然后送给外部源或者其他source,如数据可以写入到HDFS或者HBase中。
 */
class FlumePollingStreamSuite extends SparkFunSuite with BeforeAndAfter with Logging {

  val maxAttempts = 5
  val batchDuration = Seconds(1)

  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName(this.getClass.getSimpleName)
    .set("spark.streaming.clock", "org.apache.spark.util.ManualClock")

  val utils = new PollingFlumeTestUtils

  test("flume polling test") {
    testMultipleTimes(testFlumePolling)
  }

  test("flume polling test multiple hosts") {
    testMultipleTimes(testFlumePollingMultipleHost)
  }

  /**
   * Run the given test until no more java.net.BindException's are thrown.
   * 运行给定测试直到没有更多的java.net BindException的抛出
   * Do this only up to a certain attempt limit.
   * 这样做只能达到一定的尝试极限
   */
  private def testMultipleTimes(test: () => Unit): Unit = {
    var testPassed = false
    var attempt = 0
    while (!testPassed && attempt < maxAttempts) {
      try {
        test()
        testPassed = true
      } catch {
        case e: Exception if Utils.isBindCollision(e) =>
          logWarning("Exception when running flume polling test: " + e)
          attempt += 1
      }
    }
    assert(testPassed, s"Test failed after $attempt attempts!")
  }

  private def testFlumePolling(): Unit = {
    try {
      val port = utils.startSingleSink()

      writeAndVerify(Seq(port))
      utils.assertChannelsAreEmpty()
    } finally {
      utils.close()
    }
  }

  private def testFlumePollingMultipleHost(): Unit = {
    try {
      val ports = utils.startMultipleSinks()
      writeAndVerify(ports)
      utils.assertChannelsAreEmpty()
    } finally {
      utils.close()
    }
  }

  def writeAndVerify(sinkPorts: Seq[Int]): Unit = {
    // Set up the streaming context and input streams
    //设置流上下文和输入流
    val ssc = new StreamingContext(conf, batchDuration)
    val addresses = sinkPorts.map(port => new InetSocketAddress("localhost", port))
    val flumeStream: ReceiverInputDStream[SparkFlumeEvent] =
      FlumeUtils.createPollingStream(ssc, addresses, StorageLevel.MEMORY_AND_DISK,
        utils.eventsPerBatch, 5)
    val outputBuffer = new ArrayBuffer[Seq[SparkFlumeEvent]]
      with SynchronizedBuffer[Seq[SparkFlumeEvent]]
    val outputStream = new TestOutputStream(flumeStream, outputBuffer)
    outputStream.register()

    ssc.start()
    try {
      utils.sendDatAndEnsureAllDataHasBeenReceived()
      val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
      clock.advance(batchDuration.milliseconds)

      // The eventually is required to ensure that all data in the batch has been processed.
      //最终需要确保批处理中的所有数据已被处理
      eventually(timeout(10 seconds), interval(100 milliseconds)) {
        val flattenOutputBuffer = outputBuffer.flatten
        val headers = flattenOutputBuffer.map(_.event.getHeaders.map {
          case kv => (kv._1.toString, kv._2.toString)
        }).map(mapAsJavaMap)
        val bodies = flattenOutputBuffer.map(e => new String(e.event.getBody.array(), UTF_8))
        utils.assertOutput(headers, bodies)
      }
    } finally {
      ssc.stop()
    }
  }

}
