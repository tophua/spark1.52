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

package org.apache.spark.streaming

import java.io.{ObjectOutputStream, ByteArrayOutputStream, ByteArrayInputStream, File}

import scala.collection.mutable.{ArrayBuffer, SynchronizedBuffer}
import scala.reflect.ClassTag

import com.google.common.base.Charsets
import com.google.common.io.Files
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.hadoop.mapreduce.lib.output.{TextOutputFormat => NewTextOutputFormat}
import org.mockito.Mockito.mock
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark.TestUtils
import org.apache.spark.streaming.dstream.{DStream, FileInputDStream}
import org.apache.spark.streaming.scheduler._
import org.apache.spark.util.{MutableURLClassLoader, Clock, ManualClock, Utils}

/**
 * This test suites tests the checkpointing functionality of DStreams -
 * the checkpointing of a DStream's RDDs as well as the checkpointing of
 * the whole DStream graph.
 * 这个测试套件的测试dstreams检查点功能,一个dstream的RDDS检查点整个dstream的依赖关系图 
 */
class CheckpointSuite extends TestSuiteBase {

  var ssc: StreamingContext = null

  override def batchDuration: Duration = Milliseconds(500) //毫秒

  override def beforeFunction() {
    super.beforeFunction()
    Utils.deleteRecursively(new File(checkpointDir))
  }

  override def afterFunction() {
    super.afterFunction()
    if (ssc != null) ssc.stop()
    Utils.deleteRecursively(new File(checkpointDir))
  }
  //基本的RDD检查点dstream 检查点恢复
  test("basic rdd checkpoints + dstream graph checkpoint recovery") {
    //此测试的批处理时间必须为1秒.
    assert(batchDuration === Milliseconds(500), "batchDuration for this test must be 1 second")

    conf.set("spark.streaming.clock", "org.apache.spark.util.ManualClock")

    val stateStreamCheckpointInterval = Seconds(1)//秒
    val fs = FileSystem.getLocal(new Configuration())
    // this ensure checkpointing occurs at least once
    //这确保检查点至少发生一次
    //firstNumBatches=4==(1000/500)==2*2
    val firstNumBatches = (stateStreamCheckpointInterval / batchDuration).toLong * 2
    //secondNumBatches 4
    val secondNumBatches = firstNumBatches

    // Setup the streams 设置流    
    /**
     * input= Vector(List(a), List(a), List(a), List(a), List(a), List(a), List(a), List(a), List(a), List(a))
     */
    val input = (1 to 10).map(_ => Seq("a")).toSeq
    val operation = (st: DStream[String]) => {
      val updateFunc = (values: Seq[Int], state: Option[Int]) => {
        println(values.mkString(",")+"|||"+state.mkString(","))
        //
        Some(values.sum + state.getOrElse(0))
      }
      st.map(x => {
        println("======"+x)
        //x==a
        (x, 1)
        })
      //updateStateByKey可以DStream中的数据进行按key做reduce操作,然后对各个批次的数据进行累加
      .updateStateByKey(updateFunc)
      //状态流检查点间隔
      .checkpoint(stateStreamCheckpointInterval)
      .map(t =>{
        println(t._1+"|map|"+t._2)
        (t._1, t._2)
      })
    }
    var ssc = setupStreams(input, operation)
    var stateStream = ssc.graph.getOutputStreams().head.dependencies.head.dependencies.head

    // Run till a time such that at least one RDD in the stream should have been checkpointed,
    // then check whether some RDD has been checkpointed or not
    //运行到一个时间至少一个RDD在流应该已经建立,然后检查是否已建立或不见
    ssc.start()
    advanceTimeWithRealDelay(ssc, firstNumBatches)
    logInfo("Checkpoint data of state stream = \n" + stateStream.checkpointData)
    assert(!stateStream.checkpointData.currentCheckpointFiles.isEmpty,
      "No checkpointed RDDs in state stream before first failure")
    stateStream.checkpointData.currentCheckpointFiles.foreach {
      case (time, file) => {
        assert(fs.exists(new Path(file)), "Checkpoint file '" + file +"' for time " + time +
            " for state stream before first failure does not exist")
      }
    }

    // Run till a further time such that previous checkpoint files in the stream would be deleted
    // and check whether the earlier checkpoint files are deleted
    //运行到进一步的时间,这样流中的以前的检查点文件将被删除,删除之前的检查点
    val checkpointFiles = stateStream.checkpointData.currentCheckpointFiles.map(x => new File(x._2))
    advanceTimeWithRealDelay(ssc, secondNumBatches)
    checkpointFiles.foreach(file =>
      assert(!file.exists, "Checkpoint file '" + file + "' was not deleted"))
    ssc.stop()

    // Restart stream computation using the checkpoint file and check whether
    // checkpointed RDDs have been restored or not
    //重启流计算使用检查点文件,并检查是否建立RDDS已经恢复或不
    ssc = new StreamingContext(checkpointDir)
    stateStream = ssc.graph.getOutputStreams().head.dependencies.head.dependencies.head
    println("Restored data of state stream 111="+stateStream.generatedRDDs.mkString("\n"))
    logInfo("Restored data of state stream = \n[" + stateStream.generatedRDDs.mkString("\n") + "]")
    assert(!stateStream.generatedRDDs.isEmpty,
      "No restored RDDs in state stream after recovery from first failure")


    // Run one batch to generate a new checkpoint file and check whether some RDD
    // is present in the checkpoint data or not
    //运行一个批处理来生成一个新的检查点文件,检查是否有RDD是在检查点数据或不存在
    ssc.start()
    advanceTimeWithRealDelay(ssc, 1)
    assert(!stateStream.checkpointData.currentCheckpointFiles.isEmpty,
      "No checkpointed RDDs in state stream before second failure")
    stateStream.checkpointData.currentCheckpointFiles.foreach {
      case (time, file) => {
        assert(fs.exists(new Path(file)), "Checkpoint file '" + file +"' for time " + time +
          " for state stream before seconds failure does not exist")
      }
    }
    ssc.stop()

    // Restart stream computation from the new checkpoint file to see whether that file has
    // correct checkpoint data
    //从新的检查点文件中重新启动流式计算,查看该文件是否具有正确的检查点数据
    ssc = new StreamingContext(checkpointDir)
    stateStream = ssc.graph.getOutputStreams().head.dependencies.head.dependencies.head
    println("Restored data of state stream 2222="+stateStream.generatedRDDs.mkString("\n"))
    logInfo("Restored data of state stream = \n[" + stateStream.generatedRDDs.mkString("\n") + "]")
    //从第二次故障失败后,恢复没有RDDS状态流
    assert(!stateStream.generatedRDDs.isEmpty,
      "No restored RDDs in state stream after recovery from second failure")

    // Adjust manual clock time as if it is being restarted after a delay; this is a hack because
    // we modify the conf object, but it works for this one property
    //调整手动时钟的时间,延迟后重新启动,这是一个黑客,因为我们修改conf对象,但它为这一个属性
    // 3500=batchDuration(500)*5
    ssc.conf.set("spark.streaming.manualClock.jump", (batchDuration.milliseconds * 7).toString)
    ssc.start()
    advanceTimeWithRealDelay(ssc, 4)
    ssc.stop()
    ssc = null
  }

  // This tests whether spark conf persists through checkpoints, and certain
  // configs gets scrubbed 
  //测试是否通过检查点持久化Spark 配置文件,某些配置项被擦洗
  test("recovery of conf through checkpoints") {//通过检查点的conf文件恢复
    val key = "spark.mykey"
    val value = "myvalue"
    System.setProperty(key, value)
    ssc = new StreamingContext(master, framework, batchDuration)
    val originalConf = ssc.conf

    val cp = new Checkpoint(ssc, Time(1000))
    val cpConf = cp.createSparkConf()
    assert(cpConf.get("spark.master") === originalConf.get("spark.master"))
    assert(cpConf.get("spark.app.name") === originalConf.get("spark.app.name"))
    assert(cpConf.get(key) === value)
    ssc.stop()

    // Serialize/deserialize to simulate write to storage and reading it back
    //序列化/反序列化进行存储和回读写
    val newCp = Utils.deserialize[Checkpoint](Utils.serialize(cp))

    // Verify new SparkConf has all the previous properties
    //验证新的sparkconf之前的所有属性
    val newCpConf = newCp.createSparkConf()
    assert(newCpConf.get("spark.master") === originalConf.get("spark.master"))
    assert(newCpConf.get("spark.app.name") === originalConf.get("spark.app.name"))
    assert(newCpConf.get(key) === value)
    assert(!newCpConf.contains("spark.driver.host"))
    assert(!newCpConf.contains("spark.driver.port"))

    // Check if all the parameters have been restored
    //检查所有参数是否已恢复
    ssc = new StreamingContext(null, newCp, null)
    val restoredConf = ssc.conf
    assert(restoredConf.get(key) === value)
    ssc.stop()

    // Verify new SparkConf picks up new master url if it is set in the properties. See SPARK-6331.
    //验证新的sparkconf,新主URL是否设置属性
    try {
      val newMaster = "local[100]"
      System.setProperty("spark.master", newMaster)
      val newCpConf = newCp.createSparkConf()
      assert(newCpConf.get("spark.master") === newMaster)
      assert(newCpConf.get("spark.app.name") === originalConf.get("spark.app.name"))
      ssc = new StreamingContext(null, newCp, null)
      assert(ssc.sparkContext.master === newMaster)
    } finally {
      System.clearProperty("spark.master")
    }
  }

  // This tests if "spark.driver.host" and "spark.driver.port" is set by user, can be recovered
  // with correct value.可以恢复正确的值
  //获得正确的spark.driver.[host|port]来自检查点 
  test("get correct spark.driver.[host|port] from checkpoint") {
    val conf = Map("spark.driver.host" -> "localhost", "spark.driver.port" -> "9999")
    conf.foreach(kv => System.setProperty(kv._1, kv._2))
    ssc = new StreamingContext(master, framework, batchDuration)
    val originalConf = ssc.conf
    assert(originalConf.get("spark.driver.host") === "localhost")
    assert(originalConf.get("spark.driver.port") === "9999")

    val cp = new Checkpoint(ssc, Time(1000))
    ssc.stop()

    // Serialize/deserialize to simulate write to storage and reading it back
    //序列化/反序列化进行存储和回读写
    val newCp = Utils.deserialize[Checkpoint](Utils.serialize(cp))

    val newCpConf = newCp.createSparkConf()
    assert(newCpConf.contains("spark.driver.host"))
    assert(newCpConf.contains("spark.driver.port"))
    assert(newCpConf.get("spark.driver.host") === "localhost")
    assert(newCpConf.get("spark.driver.port") === "9999")

    // Check if all the parameters have been restored
    //检查所有参数是否已恢复
    ssc = new StreamingContext(null, newCp, null)
    val restoredConf = ssc.conf
    assert(restoredConf.get("spark.driver.host") === "localhost")
    assert(restoredConf.get("spark.driver.port") === "9999")
    ssc.stop()

    // If spark.driver.host and spark.driver.host is not set in system property, these two
    // parameters should not be presented in the newly recovered conf.
    //这两个参数不应该在新恢复的配置
    conf.foreach(kv => System.clearProperty(kv._1))
    val newCpConf1 = newCp.createSparkConf()
    assert(!newCpConf1.contains("spark.driver.host"))
    assert(!newCpConf1.contains("spark.driver.port"))

    // Spark itself will dispatch a random, not-used port for spark.driver.port if it is not set
    // explicitly.
    //Spark本身将派遣一个随机,不使用的端口为spark.driver.port如果没有显式设置
    ssc = new StreamingContext(null, newCp, null)
    val restoredConf1 = ssc.conf
    assert(restoredConf1.get("spark.driver.host") === "localhost")
    assert(restoredConf1.get("spark.driver.port") !== "9999")
  }

  // This tests whether the system can recover from a master failure with simple
  // non-stateful operations. This assumes as reliable, replayable input
  //本测试系统是否能从一个简单的无状态操作主机故障中恢复,这是可靠的,可重复的输入源TestInputDStream
  // source - TestInputDStream.
  test("recovery with map and reduceByKey operations") {//恢复map和reducebykey操作
    testCheckpointedOperation(
      Seq( Seq("a", "a", "b"), Seq("", ""), Seq(), Seq("a", "a", "b"), Seq("", ""), Seq() ),
      (s: DStream[String]) => s.map(x => (x, 1)).reduceByKey(_ + _),
      Seq(
        Seq(("a", 2), ("b", 1)),
        Seq(("", 2)),
        Seq(),
        Seq(("a", 2), ("b", 1)),
        Seq(("", 2)), Seq() ),
      3
    )
  }


  // This tests whether the ReduceWindowedDStream's RDD checkpoints works correctly such
  // that the system can recover from a master failure. This assumes as reliable,
  //这个测试是否reducewindoweddstream RDD检查站的正常工作,系统可以从主故障中恢复,可重复的输入源TestInputDStream
  // replayable input source - TestInputDStream.
  //恢复可逆reduceByKeyAndWindow操作
  test("recovery with invertible reduceByKeyAndWindow operation") {
    val n = 10
    val w = 4
    val input = (1 to n).map(_ => Seq("a")).toSeq
    val output = Seq(
      Seq(("a", 1)), Seq(("a", 2)), Seq(("a", 3))) ++ (1 to (n - w + 1)).map(x => Seq(("a", 4)))
    val operation = (st: DStream[String]) => {
      st.map(x => (x, 1))
        .reduceByKeyAndWindow(_ + _, _ - _, batchDuration * w, batchDuration)
        .checkpoint(batchDuration * 2)
    }
    testCheckpointedOperation(input, operation, output, 7)
  }
  //恢复saveAsHadoopFiles操作
  test("recovery with saveAsHadoopFiles operation") {
    val tempDir = Utils.createTempDir()
    try {
      testCheckpointedOperation(
        Seq(Seq("a", "a", "b"), Seq("", ""), Seq(), Seq("a", "a", "b"), Seq("", ""), Seq()),
        (s: DStream[String]) => {
          val output = s.map(x => (x, 1)).reduceByKey(_ + _)
          output.saveAsHadoopFiles(
            tempDir.toURI.toString,
            "result",
            classOf[Text],
            classOf[IntWritable],
            classOf[TextOutputFormat[Text, IntWritable]])
          output
        },
        Seq(
          Seq(("a", 2), ("b", 1)),
          Seq(("", 2)),
          Seq(),
          Seq(("a", 2), ("b", 1)),
          Seq(("", 2)),
          Seq()),
        3
      )
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }
  //恢复saveAsNewAPIHadoopFiles操作
  test("recovery with saveAsNewAPIHadoopFiles operation") {
    val tempDir = Utils.createTempDir()
    try {
      testCheckpointedOperation(
        Seq(Seq("a", "a", "b"), Seq("", ""), Seq(), Seq("a", "a", "b"), Seq("", ""), Seq()),
        (s: DStream[String]) => {
          val output = s.map(x => (x, 1)).reduceByKey(_ + _)
          output.saveAsNewAPIHadoopFiles(
            tempDir.toURI.toString,
            "result",
            classOf[Text],
            classOf[IntWritable],
            classOf[NewTextOutputFormat[Text, IntWritable]])
          output
        },
        Seq(
          Seq(("a", 2), ("b", 1)),
          Seq(("", 2)),
          Seq(),
          Seq(("a", 2), ("b", 1)),
          Seq(("", 2)),
          Seq()),
        3
      )
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }
  //恢复saveAsHadoopFile操作内部转换操作
  test("recovery with saveAsHadoopFile inside transform operation") {
    // Regression test for SPARK-4835.
    //
    // In that issue, the problem was that `saveAsHadoopFile(s)` would fail when the last batch
    // was restarted from a checkpoint since the output directory would already exist.  However,
    // the other saveAsHadoopFile* tests couldn't catch this because they only tested whether the
    // output matched correctly and not whether the post-restart batch had successfully finished
    // without throwing any errors.  The following test reproduces the same bug with a test that
    // actually fails because the error in saveAsHadoopFile causes transform() to fail, which
    // prevents the expected output from being written to the output stream.
    //
    // This is not actually a valid use of transform, but it's being used here so that we can test
    // the fix for SPARK-4835 independently of additional test cleanup.
    //
    // After SPARK-5079 is addressed, should be able to remove this test since a strengthened
    // version of the other saveAsHadoopFile* tests would prevent regressions for this issue.
    val tempDir = Utils.createTempDir()
    try {
      testCheckpointedOperation(
        Seq(Seq("a", "a", "b"), Seq("", ""), Seq(), Seq("a", "a", "b"), Seq("", ""), Seq()),
        (s: DStream[String]) => {
          s.transform { (rdd, time) =>
            val output = rdd.map(x => (x, 1)).reduceByKey(_ + _)
            output.saveAsHadoopFile(
              new File(tempDir, "result-" + time.milliseconds).getAbsolutePath,
              classOf[Text],
              classOf[IntWritable],
              classOf[TextOutputFormat[Text, IntWritable]])
            output
          }
        },
        Seq(
          Seq(("a", 2), ("b", 1)),
          Seq(("", 2)),
          Seq(),
          Seq(("a", 2), ("b", 1)),
          Seq(("", 2)),
          Seq()),
        3
      )
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }

  // This tests whether the StateDStream's RDD checkpoints works correctly such
  // that the system can recover from a master failure. This assumes as reliable,
  //这个测试是否statedstream RDD检查站的正常工作，系统可以从主故障中恢复,这是可靠的重复输入源
  // replayable input source - TestInputDStream.
  //恢复updateStateByKey操作
  test("recovery with updateStateByKey operation") {
    val input = (1 to 10).map(_ => Seq("a")).toSeq
    val output = (1 to 10).map(x => Seq(("a", x))).toSeq
    val operation = (st: DStream[String]) => {
      val updateFunc = (values: Seq[Int], state: Option[Int]) => {
        Some((values.sum + state.getOrElse(0)))
      }
      st.map(x => (x, 1))
        .updateStateByKey(updateFunc)
        .checkpoint(batchDuration * 2)
        .map(t => (t._1, t._2))
    }
    testCheckpointedOperation(input, operation, output, 7)
  }

  test("recovery maintains rate controller") {//恢复维护速率控制器
    ssc = new StreamingContext(conf, batchDuration)
    ssc.checkpoint(checkpointDir)

    val dstream = new RateTestInputDStream(ssc) {
      override val rateController =
        Some(new ReceiverRateController(id, new ConstantEstimator(200)))
    }

    val output = new TestOutputStreamWithPartitions(dstream.checkpoint(batchDuration * 2))
    output.register()
    runStreams(ssc, 5, 5)

    ssc = new StreamingContext(checkpointDir)
    ssc.start()

    eventually(timeout(10.seconds)) {
      assert(RateTestReceiver.getActive().nonEmpty)
    }

    advanceTimeWithRealDelay(ssc, 2)

    eventually(timeout(10.seconds)) {
      assert(RateTestReceiver.getActive().get.getDefaultBlockGeneratorRateLimit() === 200)
    }
    ssc.stop()
  }

  // This tests whether file input stream remembers what files were seen before
  //此测试是否文件输入流是否记得在主故障之前被看到的文件,并再次使用它们来处理一个大窗口操作
  // the master failure and uses them again to process a large window operation.
  // It also tests whether batches, whose processing was incomplete due to the
  // 它也测试是否批次,其处理是不完整的,由于故障是否重新处理
  // failure, are re-processed or not.
  test("recovery with file input stream") {//恢复文件输入流
    // Set up the streaming context and input streams
    //设置流上下文和输入流
    val batchDuration = Seconds(2)  // Due to 1-second resolution of setLastModified() on some OS's.
    val testDir = Utils.createTempDir()
    val outputBuffer = new ArrayBuffer[Seq[Int]] with SynchronizedBuffer[Seq[Int]]

    /**
     * Writes a file named `i` (which contains the number `i`) to the test directory and sets its
     * modification time to `clock`'s current time.
     * 修改时间到时钟的当前时间
     */
    def writeFile(i: Int, clock: Clock): Unit = {
      val file = new File(testDir, i.toString)
      Files.write(i + "\n", file, Charsets.UTF_8)
      assert(file.setLastModified(clock.getTimeMillis()))
      // Check that the file's modification date is actually the value we wrote, since rounding or
      // truncation will break the test:
      //请检查文件的修改日期实际上是我们写的值,由于四舍五入或截断测试
      assert(file.lastModified() === clock.getTimeMillis())
    }

    /**
     * Returns ids that identify which files which have been recorded by the file input stream.
     * 返回标识已通过文件输入流记录的文件的标识
     */
    def recordedFiles(ssc: StreamingContext): Seq[Int] = {
      val fileInputDStream =
        ssc.graph.getInputStreams().head.asInstanceOf[FileInputDStream[_, _, _]]
      val filenames = fileInputDStream.batchTimeToSelectedFiles.values.flatten
      filenames.map(_.split(File.separator).last.toInt).toSeq.sorted
    }

    try {
      // This is a var because it's re-assigned when we restart from a checkpoint
      //这是一个变量,因为它是重新分配的,当我们从一个检查点重新启动
      var clock: ManualClock = null
      withStreamingContext(new StreamingContext(conf, batchDuration)) { ssc =>
        ssc.checkpoint(checkpointDir)
        clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
        val batchCounter = new BatchCounter(ssc)
        val fileStream = ssc.textFileStream(testDir.toString)
        // Make value 3 take a large time to process, to ensure that the driver
        // shuts down in the middle of processing the 3rd batch
        //生产3获取需要大量的时间来处理,确保driver在第三批处理中间关闭
        CheckpointSuite.batchThreeShouldBlockIndefinitely = true
        val mappedStream = fileStream.map(s => {
          val i = s.toInt
          if (i == 3) {
            while (CheckpointSuite.batchThreeShouldBlockIndefinitely) {
              Thread.sleep(Long.MaxValue)
            }
          }
          i
        })

        // Reducing over a large window to ensure that recovery from driver failure
        // requires reprocessing of all the files seen before the failure
        //减少在一个大的窗口,以确保恢复从驱动程序故障,需要重新处理之前看到的故障的所有文件
        val reducedStream = mappedStream.reduceByWindow(_ + _, batchDuration * 30, batchDuration)
        val outputStream = new TestOutputStream(reducedStream, outputBuffer)
        outputStream.register()
        ssc.start()

        // Advance half a batch so that the first file is created after the StreamingContext starts
        //提前半个批,第一个文件是StreamingContext开始了
        clock.advance(batchDuration.milliseconds / 2)
        // Create files and advance manual clock to process them
        //创建文件和推进手动时钟来处理它们
        for (i <- Seq(1, 2, 3)) {
          writeFile(i, clock)
          // Advance the clock after creating the file to avoid a race when
          // setting its modification time
          //在设置修改时间后创建文件以避免竞争的时钟
          clock.advance(batchDuration.milliseconds)
          if (i != 3) {
            // Since we want to shut down while the 3rd batch is processing
            //因为我们想关闭,而第三批处理
            eventually(eventuallyTimeout) {
              assert(batchCounter.getNumCompletedBatches === i)
            }
          }
        }
        eventually(eventuallyTimeout) {
          // Wait until all files have been recorded and all batches have started
          //等待,直到所有的文件被记录,所有批次已经开始
          assert(recordedFiles(ssc) === Seq(1, 2, 3) && batchCounter.getNumStartedBatches === 3)
        }
        clock.advance(batchDuration.milliseconds)
        // Wait for a checkpoint to be written
        //等待要写的检查点
        eventually(eventuallyTimeout) {
          assert(Checkpoint.getCheckpointFiles(checkpointDir).size === 6)
        }
        ssc.stop()
        // Check that we shut down while the third batch was being processed
        //检查我们关闭,而第三批正在处理
        assert(batchCounter.getNumCompletedBatches === 2)
        assert(outputStream.output.flatten === Seq(1, 3))
      }

      // The original StreamingContext has now been stopped.
      //原来StreamingContext已经停止了
      CheckpointSuite.batchThreeShouldBlockIndefinitely = false

      // Create files while the streaming driver is down
      //创建文件,流驱动程序正在关闭
      for (i <- Seq(4, 5, 6)) {
        writeFile(i, clock)
        // Advance the clock after creating the file to avoid a race when
        //在设置修改时间后创建文件以避免竞争的时钟
        // setting its modification time
        clock.advance(batchDuration.milliseconds)
      }

      // Recover context from checkpoint file and verify whether the files that were
      // recorded before failure were saved and successfully recovered
      //从检查点文件中恢复上下文,并检查在失败之前记录的文件是否被保存并成功恢复
      logInfo("*********** RESTARTING ************")
      withStreamingContext(new StreamingContext(checkpointDir)) { ssc =>
        // "batchDuration.milliseconds * 3" has gone before restarting StreamingContext. And because
        // the recovery time is read from the checkpoint time but the original clock doesn't align
        // with the batch time, we need to add the offset "batchDuration.milliseconds / 2".
        ssc.conf.set("spark.streaming.manualClock.jump",
          (batchDuration.milliseconds / 2 + batchDuration.milliseconds * 3).toString)
        val oldClockTime = clock.getTimeMillis() // 15000ms
        clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
        val batchCounter = new BatchCounter(ssc)
        val outputStream = ssc.graph.getOutputStreams().head.asInstanceOf[TestOutputStream[Int]]
        // Check that we remember files that were recorded before the restart
        //请检查我们记得在重新启动前记录的文件
        assert(recordedFiles(ssc) === Seq(1, 2, 3))

        // Restart stream computation
        //重启流计算
        ssc.start()
        // Verify that the clock has traveled forward to the expected time
        //确认时钟已经向前走到预期的时间
        eventually(eventuallyTimeout) {
          assert(clock.getTimeMillis() === oldClockTime)
        }
        // There are 5 batches between 6000ms and 15000ms (inclusive).
        //有5个批次6000ms和15000ms之间(含)
        val numBatchesAfterRestart = 5
        eventually(eventuallyTimeout) {
          assert(batchCounter.getNumCompletedBatches === numBatchesAfterRestart)
        }
        for ((i, index) <- Seq(7, 8, 9).zipWithIndex) {
          writeFile(i, clock)
          // Advance the clock after creating the file to avoid a race when
          // setting its modification time
          clock.advance(batchDuration.milliseconds)
          eventually(eventuallyTimeout) {
            assert(batchCounter.getNumCompletedBatches === index + numBatchesAfterRestart + 1)
          }
        }
        logInfo("Output after restart = " + outputStream.output.mkString("[", ", ", "]"))
        assert(outputStream.output.size > 0, "No files processed after restart")
        ssc.stop()

        // Verify whether files created while the driver was down (4, 5, 6) and files created after
        // recovery (7, 8, 9) have been recorded
        //验证是否创建的文件,而驱动程序是下降(4，5，6)和恢复后创建的文件(7，8，9)已被记录
        assert(recordedFiles(ssc) === (1 to 9))

        // Append the new output to the old buffer
        //将新的输出添加到旧缓冲区
        outputBuffer ++= outputStream.output

        // Verify whether all the elements received are as expected
        //检查是否所有接收到的元素是否如预期的
        val expectedOutput = Seq(1, 3, 6, 10, 15, 21, 28, 36, 45)
        assert(outputBuffer.flatten.toSet === expectedOutput.toSet)
      }
    } finally {
      Utils.deleteRecursively(testDir)
    }
  }

  //批处理中的两个检查站竞争条件
  test("SPARK-11267: the race condition of two checkpoints in a batch") {
    val jobGenerator = mock(classOf[JobGenerator])
    val checkpointDir = Utils.createTempDir().toString
    val checkpointWriter =
      new CheckpointWriter(jobGenerator, conf, checkpointDir, new Configuration())
    val bytes1 = Array.fill[Byte](10)(1)
    new checkpointWriter.CheckpointWriteHandler(
      Time(2000), bytes1, clearCheckpointDataLater = false).run()
    val bytes2 = Array.fill[Byte](10)(2)
    new checkpointWriter.CheckpointWriteHandler(
      Time(1000), bytes2, clearCheckpointDataLater = true).run()
    val checkpointFiles = Checkpoint.getCheckpointFiles(checkpointDir).reverse.map { path =>
      new File(path.toUri)
    }
    assert(checkpointFiles.size === 2)
    // Although bytes2 was written with an old time, it contains the latest status, so we should
    // try to read from it at first.
    //虽然bytes2写的是一个旧时间,它包含了最新的状态,所以我们应该先从中读出,
    assert(Files.toByteArray(checkpointFiles(0)) === bytes2)
    assert(Files.toByteArray(checkpointFiles(1)) === bytes1)
    checkpointWriter.stop()
  }

  /**
   * Tests a streaming operation under checkpointing, by restarting the operation
   * 在流式操作测试检查点,通过重新启动检查点文件的操作,并检查最终输出是否正确
   * from checkpoint file and verifying whether the final output is correct.
   * The output is assumed to have come from a reliable queue which an replay
   * data as required.
   *输出被假定为来自一个可靠的队列,所需的重播数据
   * 注意:这考虑到最后一批处理前主故障将重新处理后重新启动/恢复
   * NOTE: This takes into consideration that the last batch processed before
   * master failure will be re-processed after restart/recovery.
   */
  def testCheckpointedOperation[U: ClassTag, V: ClassTag](
    input: Seq[Seq[U]],
    operation: DStream[U] => DStream[V],
    expectedOutput: Seq[Seq[V]],
    initialNumBatches: Int
  ) {

    // Current code assumes that:
    //当前代码假定:
    // number of inputs = number of outputs = number of batches to be run
    //输入=输出数=要运行的批数
    val totalNumBatches = input.size
    val nextNumBatches = totalNumBatches - initialNumBatches
    val initialNumExpectedOutputs = initialNumBatches
    val nextNumExpectedOutputs = expectedOutput.size - initialNumExpectedOutputs + 1
    // because the last batch will be processed again
    //因为最后一批将再次被处理
    // Do the computation for initial number of batches, create checkpoint file and quit
    //对初始批数进行计算,创建检查点文件并退出
    ssc = setupStreams[U, V](input, operation)
    ssc.start()
    val output = advanceTimeWithRealDelay[V](ssc, initialNumBatches)
    ssc.stop()
    verifyOutput[V](output, expectedOutput.take(initialNumBatches), true)
    Thread.sleep(1000)

    // Restart and complete the computation from checkpoint file
    //重新启动并完成检查点文件的计算
    logInfo(
      "\n-------------------------------------------\n" +
      "        Restarting stream computation          " +
      "\n-------------------------------------------\n"
    )
    ssc = new StreamingContext(checkpointDir)
    ssc.start()
    val outputNew = advanceTimeWithRealDelay[V](ssc, nextNumBatches)
    // the first element will be re-processed data of the last batch before restart
    //重新启动前的最后一批处理的第一个元素将被重新处理
    verifyOutput[V](outputNew, expectedOutput.takeRight(nextNumExpectedOutputs), true)
    ssc.stop()
    ssc = null
  }

  /**
   * Advances the manual clock on the streaming scheduler by given number of batches.
   * It also waits for the expected amount of time for each batch.
   * 通过给定的批数,将人工时钟在流式调度上,它等待每个批次的预期时间
   */
  def advanceTimeWithRealDelay[V: ClassTag](ssc: StreamingContext, numBatches: Long): Seq[Seq[V]] =
  {
    val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    //提前的手动时钟
    logInfo("Manual clock before advancing = " + clock.getTimeMillis())
    for (i <- 1 to numBatches.toInt) {
      clock.advance(batchDuration.milliseconds)
      Thread.sleep(batchDuration.milliseconds)
    }
    //提前后的手动时钟
    logInfo("Manual clock after advancing = " + clock.getTimeMillis())
    Thread.sleep(batchDuration.milliseconds)

    val outputStream = ssc.graph.getOutputStreams().filter { dstream =>
      dstream.isInstanceOf[TestOutputStreamWithPartitions[V]]
    }.head.asInstanceOf[TestOutputStreamWithPartitions[V]]
    outputStream.output.map(_.flatten)
  }
}

private object CheckpointSuite extends Serializable {
  var batchThreeShouldBlockIndefinitely: Boolean = true
}
