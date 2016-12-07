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

package org.apache.spark.streaming.receiver

import scala.collection.mutable

import org.scalatest.BeforeAndAfter
import org.scalatest.Matchers._
import org.scalatest.concurrent.Timeouts._
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark.storage.StreamBlockId
import org.apache.spark.util.ManualClock
import org.apache.spark.{SparkException, SparkConf, SparkFunSuite}

class BlockGeneratorSuite extends SparkFunSuite with BeforeAndAfter {

  private val blockIntervalMs = 10 //间隔10毫秒
  //Spark Streaming接收器将接收数据合并成数据块并存储在Spark里的时间间隔，毫秒
  private val conf = new SparkConf().set("spark.streaming.blockInterval", s"${blockIntervalMs}ms")
  @volatile private var blockGenerator: BlockGenerator = null

  after {
    if (blockGenerator != null) {
      blockGenerator.stop()
    }
  }

  test("block generation and data callbacks") {//块生成和数据回调
    val listener = new TestBlockGeneratorListener
    val clock = new ManualClock()

    require(blockIntervalMs > 5)
    require(listener.onAddDataCalled === false)
    require(listener.onGenerateBlockCalled === false)
    require(listener.onPushBlockCalled === false)

    // Verify that creating the generator does not start it
    //确认创建生成器不启动它
    blockGenerator = new BlockGenerator(listener, 0, conf, clock)
    assert(blockGenerator.isActive() === false, "block generator active before start()")
    assert(blockGenerator.isStopped() === false, "block generator stopped before start()")
    assert(listener.onAddDataCalled === false)
    assert(listener.onGenerateBlockCalled === false)
    assert(listener.onPushBlockCalled === false)

    // Verify start marks the generator active, but does not call the callbacks
    //验证的启动标志,但不会调用回调
    blockGenerator.start()
    assert(blockGenerator.isActive() === true, "block generator active after start()")
    assert(blockGenerator.isStopped() === false, "block generator stopped after start()")
    withClue("callbacks called before adding data") {
      assert(listener.onAddDataCalled === false)
      assert(listener.onGenerateBlockCalled === false)
      assert(listener.onPushBlockCalled === false)
    }

    // Verify whether addData() adds data that is present in generated blocks
    //验证是否addData() 添加数据在目前的数据生成模块
    val data1 = 1 to 10
    data1.foreach { blockGenerator.addData _ }
    withClue("callbacks called on adding data without metadata and without block generation") {
      assert(listener.onAddDataCalled === false) // should be called only with addDataWithCallback()
      assert(listener.onGenerateBlockCalled === false)
      assert(listener.onPushBlockCalled === false)
    }
    //提前时钟产生块
    clock.advance(blockIntervalMs)  // advance clock to generate blocks
    withClue("blocks not generated or pushed") {
      eventually(timeout(1 second)) {
        assert(listener.onGenerateBlockCalled === true)
        assert(listener.onPushBlockCalled === true)
      }
    }
    listener.pushedData should contain theSameElementsInOrderAs (data1)
    assert(listener.onAddDataCalled === false) // should be called only with addDataWithCallback()

    // Verify addDataWithCallback() add data+metadata and and callbacks are called correctly
    val data2 = 11 to 20
    val metadata2 = data2.map { _.toString }
    data2.zip(metadata2).foreach { case (d, m) => blockGenerator.addDataWithCallback(d, m) }
    assert(listener.onAddDataCalled === true)
    listener.addedData should contain theSameElementsInOrderAs (data2)
    listener.addedMetadata should contain theSameElementsInOrderAs (metadata2)
    //提前时钟产生块
    clock.advance(blockIntervalMs)  // advance clock to generate blocks
    eventually(timeout(1 second)) {
      listener.pushedData should contain theSameElementsInOrderAs (data1 ++ data2)
    }

    // Verify addMultipleDataWithCallback() add data+metadata and and callbacks are called correctly
    val data3 = 21 to 30
    val metadata3 = "metadata"
    blockGenerator.addMultipleDataWithCallback(data3.iterator, metadata3)
    listener.addedMetadata should contain theSameElementsInOrderAs (metadata2 :+ metadata3)
    clock.advance(blockIntervalMs)  // advance clock to generate blocks
    eventually(timeout(1 second)) {
      listener.pushedData should contain theSameElementsInOrderAs (data1 ++ data2 ++ data3)
    }

    // Stop the block generator by starting the stop on a different thread and
    // then advancing the manual clock for the stopping to proceed.
    //通过在另一个线程上启动停止,停止块生成器然后推送手动时钟停止继续进行
    val thread = stopBlockGenerator(blockGenerator)
    eventually(timeout(1 second), interval(10 milliseconds)) {
      clock.advance(blockIntervalMs)
      assert(blockGenerator.isStopped() === true)
    }
    thread.join()

    // Verify that the generator cannot be used any more
    //确认产生不能再使用
    intercept[SparkException] {
      blockGenerator.addData(1)
    }
    intercept[SparkException] {
      blockGenerator.addDataWithCallback(1, 1)
    }
    intercept[SparkException] {
      blockGenerator.addMultipleDataWithCallback(Iterator(1), 1)
    }
    intercept[SparkException] {
      blockGenerator.start()
    }
    //应该调用停止
    blockGenerator.stop()   // Calling stop again should be fine
  }

  test("stop ensures correct shutdown") {//确保正确的关闭
    val listener = new TestBlockGeneratorListener
    val clock = new ManualClock()//手动时钟
    blockGenerator = new BlockGenerator(listener, 0, conf, clock)
    require(listener.onGenerateBlockCalled === false)
    blockGenerator.start()
    assert(blockGenerator.isActive() === true, "block generator")
    assert(blockGenerator.isStopped() === false)

    val data = 1 to 1000
    data.foreach { blockGenerator.addData _ }

    // Verify that stop() shutdowns everything in the right order
    //验证stop()关闭所有的事情以正确的顺序
    // - First, stop receiving new data
    // - 第一,停止接收新的数据
    // - Second, wait for final block with all buffered data to be generated
    // - 第二,等待最后一个块与所有缓冲的数据生成
    // - Finally, wait for all blocks to be pushed
    // - 最后,等待所有的块被推
    //确保定时器的另一个间隔完成
    clock.advance(1) // to make sure that the timer for another interval to complete
    
    val thread = stopBlockGenerator(blockGenerator)
    eventually(timeout(1 second), interval(10 milliseconds)) {
      assert(blockGenerator.isActive() === false)
    }
    assert(blockGenerator.isStopped() === false)

    // Verify that data cannot be added
    //无法添加数据验证
    intercept[SparkException] {
      blockGenerator.addData(1)
    }
    intercept[SparkException] {
      blockGenerator.addDataWithCallback(1, null)
    }
    intercept[SparkException] {
      blockGenerator.addMultipleDataWithCallback(Iterator(1), null)
    }

    // Verify that stop() stays blocked until another block containing all the data is generated
    //验证stop()停留阻塞直到生成另一块包含的所有数据,这拦截总是成功,由于块要么将抛出一个超时异常
    // This intercept always succeeds, as the body either will either throw a timeout exception
    // (expected as stop() should never complete) or a SparkException (unexpected as stop()
    // completed and thread terminated).
    val exception = intercept[Exception] {
      failAfter(200 milliseconds) {
        thread.join()
        throw new SparkException(
          "BlockGenerator.stop() completed before generating timer was stopped")
      }
    }
    exception should not be a [SparkException]


    // Verify that the final data is present in the final generated block and
    // pushed before complete stop
    //确认最终的数据是存在于最终生成的块,并在完全停止之前推送的
    //产生还没有停止
    assert(blockGenerator.isStopped() === false) // generator has not stopped yet
    eventually(timeout(10 seconds), interval(10 milliseconds)) {
      // Keep calling `advance` to avoid blocking forever in `clock.waitTillTime`
      //保持调用`advance` 为了避免永远堵塞 `clock.waitTillTime`
      clock.advance(blockIntervalMs)
      assert(thread.isAlive === false)
    }
    //产生器终于被完全停止
    assert(blockGenerator.isStopped() === true) // generator has finally been completely stopped
    assert(listener.pushedData === data, "All data not pushed by stop()")
  }

  test("block push errors are reported") {//块推送错误报告
    val listener = new TestBlockGeneratorListener {
      /**
       * Volatile修饰的成员变量在每次被线程访问时,都强迫从共享内存中重读该成员变量的值。
       * 而且,当成员变量发生变化时,强迫线程将变化值回写到共享内存
       */
      @volatile var errorReported = false  //
      override def onPushBlock(
          blockId: StreamBlockId, arrayBuffer: mutable.ArrayBuffer[_]): Unit = {
        throw new SparkException("test")
      }
      override def onError(message: String, throwable: Throwable): Unit = {
        errorReported = true
      }
    }
    blockGenerator = new BlockGenerator(listener, 0, conf)
    blockGenerator.start()
    assert(listener.errorReported === false)
    blockGenerator.addData(1)
    eventually(timeout(1 second), interval(10 milliseconds)) {
      assert(listener.errorReported === true)
    }
    blockGenerator.stop()
  }

  /**
   * Helper method to stop the block generator with manual clock in a different thread,
   * 在另一个线程中停止使用手动时钟的块发生器
   * so that the main thread can advance the clock that allows the stopping to proceed.
   * 主线程可以提前时钟,允许停止进行
   */
  private def stopBlockGenerator(blockGenerator: BlockGenerator): Thread = {
    val thread = new Thread() {
      override def run(): Unit = {
        blockGenerator.stop()
      }
    }
    thread.start()
    thread
  }

  /** 
   *  A listener for BlockGenerator that records the data in the callbacks 
   *  一个块产生器记录数据在回调监听器
   **/
  private class TestBlockGeneratorListener extends BlockGeneratorListener {
    //
    val pushedData = new mutable.ArrayBuffer[Any] with mutable.SynchronizedBuffer[Any]
    val addedData = new mutable.ArrayBuffer[Any] with mutable.SynchronizedBuffer[Any]
    val addedMetadata = new mutable.ArrayBuffer[Any] with mutable.SynchronizedBuffer[Any]
    @volatile var onGenerateBlockCalled = false
    @volatile var onAddDataCalled = false
    @volatile var onPushBlockCalled = false
    //取出数据块
    override def onPushBlock(blockId: StreamBlockId, arrayBuffer: mutable.ArrayBuffer[_]): Unit = {
      pushedData ++= arrayBuffer
      onPushBlockCalled = true
    }
    override def onError(message: String, throwable: Throwable): Unit = {}
    //产生数据块
    override def onGenerateBlock(blockId: StreamBlockId): Unit = {
      onGenerateBlockCalled = true
    }
    //增加数据
    override def onAddData(data: Any, metadata: Any): Unit = {
      addedData += data
      addedMetadata += metadata
      onAddDataCalled = true
    }
  }
}
