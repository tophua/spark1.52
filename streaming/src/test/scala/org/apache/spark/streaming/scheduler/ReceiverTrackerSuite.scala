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

package org.apache.spark.streaming.scheduler

import scala.collection.mutable.ArrayBuffer

import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskStart, TaskLocality}
import org.apache.spark.scheduler.TaskLocality.TaskLocality
import org.apache.spark.storage.{StorageLevel, StreamBlockId}
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver._

/** 
 *  Testsuite for receiver schedulin
 *  
 * JobScheduler将每个batch的RDD DAG具体生成工作委托给JobGenerator,
 * 而将源头输入数据的记录工作委托给ReceiverTracker
 * Spark Streaming在程序刚开始运行时,
 * 1,由Receiver的总指挥ReceiverTracker分发多个job(每个job有1个task),到多个executor上分别启动ReceiverSupervisor实例
 * 2,每个executoru端ReceiverSupervisor启动后将马上生成一个用户提供的Receiver实现的实例--
 * 	  该Receiver实现可以持续产生或者持续接收系统外的数据,比如TwitterReceiver可以实时爬取Twitter的数据,
 * 	  并在Receiver实例生成后调用Receiver.onStart()
 * 3,Receiver启动后,就将持续不断地接收外界数据,并持续交给ReceiverSupervisor进行数据转储
 * 4,ReceiverSupervisor持续不断地接收到Receiver转来的数据:
 * 		1,如果数据很细小,就需要BlockGenerator攒多条数据成一块BlockGenerator,
 * 			然后再生成块存储BlockManagerBasedBlockHandler或者WriteAheadLogBasedBlockHandler
 * 			
 * 		2,Spark目前支持两种生成块存储方式:一种是由blockManagerBaseHandler直接存到executor的内存或者硬盘
 *       另一种由WriteAheadLogBasedBlockHander是同时写WAL(预写式日志)和executor的内存或硬盘
 * 5,每次生成块在executor存储完毕后,ReceiverSupervisor就会及时上报块数据的meta信息给Driver端的
 *    ReceiverTracker这里的meta信息包括数据的标识ID,数据的位置,数据的条数,数据的大小等信息
 * 6,ReceiverTracker再将收到的块数据meta信息直接转给自己的成员ReceivedBlockTracker,由ReceivedBlockTracker专门
 *   管理收到的数据meta信息.
 *   
 *  driver 端长时容错
 *  块数据的 meta信息上报到 ReceiverTracker,然后交给 ReceivedBlockTracker做具体的管理。
 *  ReceivedBlockTracker也采用 WAL冷备方式进行备份,在 driver失效后,
 *  由新的 ReceivedBlockTracker读取 WAL并恢复 block的 meta信息
 **/

class ReceiverTrackerSuite extends TestSuiteBase {
  //发送速率更新到接收器
  test("send rate update to receivers") {
    withStreamingContext(new StreamingContext(conf, Milliseconds(100))) { ssc =>
      ssc.scheduler.listenerBus.start(ssc.sc)

      val newRateLimit = 100L
      val inputDStream = new RateTestInputDStream(ssc)
      val tracker = new ReceiverTracker(ssc)
      tracker.start()
      try {
        // we wait until the Receiver has registered with the tracker,
        //我们等待,直到接收器已注册的跟踪
        // otherwise our rate update is lost
        //否则我们的速度更新丢失
        eventually(timeout(5 seconds)) {
          assert(RateTestReceiver.getActive().nonEmpty)
        }


        // Verify that the rate of the block generator in the receiver get updated
        //确认接收器中的块生成器的速率得到更新
        val activeReceiver = RateTestReceiver.getActive().get
        tracker.sendRateUpdate(inputDStream.id, newRateLimit)
        eventually(timeout(5 seconds)) {
          assert(activeReceiver.getDefaultBlockGeneratorRateLimit() === newRateLimit,
            "default block generator did not receive rate update")
          assert(activeReceiver.getCustomBlockGeneratorRateLimit() === newRateLimit,
            "other block generator did not receive rate update")
        }
      } finally {
        tracker.stop(false)
      }
    }
  }

  test("should restart receiver after stopping it") {//停止后应重新启动接收器
    withStreamingContext(new StreamingContext(conf, Milliseconds(100))) { ssc =>
      @volatile var startTimes = 0
      ssc.addStreamingListener(new StreamingListener {
        override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = {
          startTimes += 1
        }
      })
      val input = ssc.receiverStream(new StoppableReceiver)
      val output = new TestOutputStream(input)
      //将当前DStream注册到DStreamGraph的输出流中
      output.register()
      ssc.start()
      StoppableReceiver.shouldStop = true
      eventually(timeout(10 seconds), interval(10 millis)) {
        //接收器被停止一次,所以如果它被重新启动,它应该开始两次。
        // The receiver is stopped once, so if it's restarted, it should be started twice.
        assert(startTimes === 2)
      }
    }
  }
  //tasksetmanager应该使用接收机RDD的首选地点
  test("SPARK-11063: TaskSetManager should use Receiver RDD's preferredLocations") {
    // Use ManualClock to prevent from starting batches so that we can make sure the only task is
    // for starting the Receiver
    val _conf = conf.clone.set("spark.streaming.clock", "org.apache.spark.util.ManualClock")
    //分隔的时间叫作批次间隔
    withStreamingContext(new StreamingContext(_conf, Milliseconds(100))) { ssc =>
      @volatile var receiverTaskLocality: TaskLocality = null
      ssc.sparkContext.addSparkListener(new SparkListener {
        override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
          receiverTaskLocality = taskStart.taskInfo.taskLocality
        }
      })
      val input = ssc.receiverStream(new TestReceiver)
      val output = new TestOutputStream(input)
      //将当前DStream注册到DStreamGraph的输出流中
      output.register()
      ssc.start()
      eventually(timeout(10 seconds), interval(10 millis)) {
        // If preferredLocations is set correctly, receiverTaskLocality should be NODE_LOCAL
        //如果设置了正确的首选位置,接收任务的地方应该node_local
        assert(receiverTaskLocality === TaskLocality.NODE_LOCAL)
      }
    }
  }
}

/** 
 *  An input DStream with for testing rate controlling
 *  输入dstream与测试速率控制 
 *  */
private[streaming] class RateTestInputDStream(@transient ssc_ : StreamingContext)
  extends ReceiverInputDStream[Int](ssc_) {

  override def getReceiver(): Receiver[Int] = new RateTestReceiver(id)

  @volatile
  var publishedRates = 0

  override val rateController: Option[RateController] = {
    Some(new RateController(id, new ConstantEstimator(100)) {
      override def publish(rate: Long): Unit = {
        publishedRates += 1
      }
    })
  }
}

/** 
 *  A receiver implementation for testing rate controlling 
 *  一种用于测试速率控制的接收器
 *  */
private[streaming] class RateTestReceiver(receiverId: Int, host: Option[String] = None)
  extends Receiver[Int](StorageLevel.MEMORY_ONLY) {

  private lazy val customBlockGenerator = supervisor.createBlockGenerator(
    new BlockGeneratorListener {
      override def onPushBlock(blockId: StreamBlockId, arrayBuffer: ArrayBuffer[_]): Unit = {}
      override def onError(message: String, throwable: Throwable): Unit = {}
      override def onGenerateBlock(blockId: StreamBlockId): Unit = {}
      override def onAddData(data: Any, metadata: Any): Unit = {}
    }
  )

  setReceiverId(receiverId)

  override def onStart(): Unit = {
    customBlockGenerator
    RateTestReceiver.registerReceiver(this)
  }

  override def onStop(): Unit = {
    RateTestReceiver.deregisterReceiver()
  }

  override def preferredLocation: Option[String] = host

  def getDefaultBlockGeneratorRateLimit(): Long = {
    supervisor.getCurrentRateLimit
  }

  def getCustomBlockGeneratorRateLimit(): Long = {
    customBlockGenerator.getCurrentLimit
  }
}

/**
 * A helper object to RateTestReceiver that give access to the currently active RateTestReceiver
 * instance.
 * 一个帮助对象为RateTestReceiver提供目前活跃的RateTestReceiver实例
 */
private[streaming] object RateTestReceiver {
  @volatile private var activeReceiver: RateTestReceiver = null

  def registerReceiver(receiver: RateTestReceiver): Unit = {
    activeReceiver = receiver
  }

  def deregisterReceiver(): Unit = {
    activeReceiver = null
  }

  def getActive(): Option[RateTestReceiver] = Option(activeReceiver)
}

/**
 * A custom receiver that could be stopped via StoppableReceiver.shouldStop
 * 可以通过一个自定义的接收器可以停止通过
 */
class StoppableReceiver extends Receiver[Int](StorageLevel.MEMORY_ONLY) {

  var receivingThreadOption: Option[Thread] = None

  def onStart() {
    val thread = new Thread() {
      override def run() {
        while (!StoppableReceiver.shouldStop) {
          Thread.sleep(10)
        }
        StoppableReceiver.this.stop("stop")
      }
    }
    thread.start()
  }

  def onStop() {
    StoppableReceiver.shouldStop = true
    receivingThreadOption.foreach(_.join())
    // Reset it so as to restart it
    //重新启动它，以便重新启动它
    StoppableReceiver.shouldStop = false
  }
}

object StoppableReceiver {
  @volatile var shouldStop = false
}
