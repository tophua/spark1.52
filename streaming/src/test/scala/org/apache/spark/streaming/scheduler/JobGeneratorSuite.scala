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

import java.util.concurrent.CountDownLatch

import scala.concurrent.duration._
import scala.language.postfixOps

import org.scalatest.concurrent.Eventually._

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.util.{ManualClock, Utils}
/**
 * 在 Spark Streaming里,总体负责动态作业调度的具体类是 JobScheduler
 * JobScheduler将每个batch的RDD DAG具体生成工作委托给JobGenerator
 * 而将源头输入数据的记录工作委托给ReceiverTracker
 * JobScheduler 维护一个定时器,周期就是batchDuration,定时为每个batch生成RDD DAG的实体;
 * 每次 RDD DAG 实际生成包含 5 个步骤
 * (1) 要求 ReceiverTracker将目前已收到的数据进行一次分配,即将上次 batch切分后的数据切分到到本次新的 batch里;
 * (2) 要求 DStreamGraph复制出一套新的 RDD DAG 的实例,具体过程是：DStreamGraph将要求图里的尾 DStream节点生成具体的 RDD实例
 * 		 并递归的调用尾 DStream 的上游 DStream 节点……以此遍历整个 DStreamGraph，遍历结束也就正好生成了 RDD DAG 的实例;
 * (3) 获取第 1 步 ReceiverTracker分配到本 batch的源头数据的 meta信息;
 * (4) 将第 2 步生成的本 batch 的 RDD DAG,和第 3 步获取到的 meta 信息,一同提交给 JobScheduler 异步执行;
 * (5) 只要提交结束(不管是否已开始异步执行),就马上对整个系统的当前运行状态做一个 checkpoint。
 */
class JobGeneratorSuite extends TestSuiteBase {

  // SPARK-6222 is a tricky regression bug which causes received block metadata
  // to be deleted before the corresponding batch has completed. This occurs when
  //在相应的批处理完成之前，它会导致接收到的块元数据被删除,
  // the following conditions are met.
  //当下列条件满足时,会发生此情况
  // 1. streaming checkpointing is enabled by setting streamingContext.checkpoint(dir)
  //    流是启用的设置检查点
  // 2. input data is received through a receiver as blocks 
  //    输入数据通过接收器作为块接收
  // 3. a batch processing a set of blocks takes a long time, such that a few subsequent
  //    batches have been generated and submitted for processing.
  //   成批处理一组块需要很长的时间,这样,已经产生了一些后续批次并提交处理
  //
  // The JobGenerator (as of Mar 16, 2015) checkpoints twice per batch, once after generation
  // of a batch, and another time after the completion of a batch. The cleanup of
  //一批一批,完成一批后的第二批,检查点数据的清除,Stream必须在第二点已建成,也就是说，批处理后已完全处理
  // checkpoint data (including block metadata, etc.) from DStream must be done only after the
  // 2nd checkpoint has completed, that is, after the batch has been completely processed.
  // However, the issue is that the checkpoint data and along with it received block data is
  //然而,问题是，检查点数据和它的接收块数据一起被清理，即使在第一个检查点的情况下
  // cleaned even in the case of the 1st checkpoint, causing pre-mature deletion of received block
  //导致接收块数据的预成熟删除,例如,如果第三批仍然正在处理,第七批可能会产生
  // data. For example, if the 3rd batch is still being process, the 7th batch may get generated,
  // and the corresponding "1st checkpoint" will delete received block metadata of batch older
  // 和相应的“第一检查点”将删除收到的块元数据的批处理年龄大于第六批
  // than 6th batch. That, is 3rd batch's block metadata gets deleted even before 3rd batch has
  // been completely processed.
  // 这是第三批的块的元数据获取甚至前第三批已被删除
  // This test tries to create that scenario by the following.
  // 这个测试试图通过以下来创建这个场景
  // 1. enable checkpointing 启用检查点
  // 2. generate batches with received blocks
  //    生成接收块的批处理
  // 3. make the 3rd batch never complete
  //    使第三批永不完成
  // 4. allow subsequent batches to be generated (to allow premature deletion of 3rd batch metadata)
  //    允许后续的批量生成,(允许提前删除第三批元数据)
  // 5. verify whether 3rd batch's block metadata still exists
  //    是否验证第三批的块元数据是否仍然存在
  test("SPARK-6222: Do not clear received block data too soon(立即,马上)") {//不立即清除接收块数据
    import JobGeneratorSuite._
    val checkpointDir = Utils.createTempDir()
    val testConf = conf
    testConf.set("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")
    //滚动间隔
    testConf.set("spark.streaming.receiver.writeAheadLog.rollingInterval", "1")

    withStreamingContext(new StreamingContext(testConf, batchDuration)) { ssc =>
      val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
      val numBatches = 10
      val longBatchNumber = 3 // 3rd batch will take a long time 第三批将需要很长的时间
      val longBatchTime = longBatchNumber * batchDuration.milliseconds

      val testTimeout = timeout(10 seconds)
      val inputStream = ssc.receiverStream(new TestReceiver)

      inputStream.foreachRDD((rdd: RDD[Int], time: Time) => {
        if (time.milliseconds == longBatchTime) {
          while (waitLatch.getCount() > 0) {
            waitLatch.await()
          }
        }
      })

      val batchCounter = new BatchCounter(ssc)
      ssc.checkpoint(checkpointDir.getAbsolutePath)
      ssc.start()

      // Make sure the only 1 batch of information is to be remembered
      //确保只有1个批次的信息被记住
      assert(inputStream.rememberDuration === batchDuration)
      val receiverTracker = ssc.scheduler.receiverTracker

      // Get the blocks belonging to a batch
      //获取属于一个批的块
      def getBlocksOfBatch(batchTime: Long): Seq[ReceivedBlockInfo] = {
        receiverTracker.getBlocksOfBatchAndStream(Time(batchTime), inputStream.id)
      }

      // Wait for new blocks to be received
      //等待新的块被接收
      def waitForNewReceivedBlocks() {
        eventually(testTimeout) {
          assert(receiverTracker.hasUnallocatedBlocks)
        }
      }

      // Wait for received blocks to be allocated to a batch
      //等待接收的块被分配给一个批处理
      def waitForBlocksToBeAllocatedToBatch(batchTime: Long) {
        eventually(testTimeout) {
          assert(getBlocksOfBatch(batchTime).nonEmpty)
        }
      }

      // Generate a large number of batches with blocks in them
      //产生大量的批量块
      for (batchNum <- 1 to numBatches) {
        waitForNewReceivedBlocks()
        clock.advance(batchDuration.milliseconds)
        waitForBlocksToBeAllocatedToBatch(clock.getTimeMillis())
      }

      // Wait for 3rd batch to start
      //等待第三批开始
      eventually(testTimeout) {
        ssc.scheduler.getPendingTimes().contains(Time(numBatches * batchDuration.milliseconds))
      }

      // Verify that the 3rd batch's block data is still present while the 3rd batch is incomplete
      //验证第三批的块数据仍然存在,而第三批处理是不完整的
      assert(getBlocksOfBatch(longBatchTime).nonEmpty, "blocks of incomplete batch already deleted")
      assert(batchCounter.getNumCompletedBatches < longBatchNumber)
      waitLatch.countDown()
    }
  }
}

object JobGeneratorSuite {
  val waitLatch = new CountDownLatch(1)
}
