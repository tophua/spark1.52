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

import java.io.File
import java.util.concurrent.TimeUnit

import com.google.common.base.Charsets._
import com.google.common.io.Files

import org.apache.hadoop.io.{BytesWritable, LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.{TextInputFormat => NewTextInputFormat}
import org.apache.spark.util.Utils

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import org.scalatest.Matchers._

class SparkContextSuite extends SparkFunSuite with LocalSparkContext {

  test("Only one SparkContext may be active at a time") {//只有一个sparkcontext是激活一次
    // Regression test for SPARK-4180
    val conf = new SparkConf().setAppName("test").setMaster("local")
    //allowMultipleContexts是否允许存在多个SparkContext实例的标识
      .set("spark.driver.allowMultipleContexts", "false")
    sc = new SparkContext(conf)
    // A SparkContext is already running, so we shouldn't be able to create a second one
    //一个sparkcontext已经运行,因此,我们不应该能够创建第二个
    intercept[SparkException] { new SparkContext(conf) }
    // After stopping the running context, we should be able to create a new one
    //停止运行上下文后,我们应该能够创造一个新的
    resetSparkContext()
    sc = new SparkContext(conf)
  }
  //仍然失败后,构建一个新的sparkcontext
  test("Can still construct a new SparkContext after failing to construct a previous one") {
    val conf = new SparkConf().set("spark.driver.allowMultipleContexts", "false")
    // This is an invalid configuration (no app name or master URL)
    //这是一个无效的配置,没有App名称或者主节点URL
    intercept[SparkException] {
      new SparkContext(conf)
    }
    // Even though those earlier calls failed, we should still be able to create a new context
    //即使早期的调用失败了,我们仍然应该能够创建一个新的上下文
    sc = new SparkContext(conf.setMaster("local").setAppName("test"))
  }
  //检查多个sparkcontexts可以禁用通过非法调试选项
  test("Check for multiple SparkContexts can be disabled via undocumented debug option") {
    //undocumented 正式文件
    var secondSparkContext: SparkContext = null
    try {
      val conf = new SparkConf().setAppName("test").setMaster("local")
        .set("spark.driver.allowMultipleContexts", "true")
      sc = new SparkContext(conf)
      secondSparkContext = new SparkContext(conf)
    } finally {
      Option(secondSparkContext).foreach(_.stop())
    }
  }

  test("Test getOrCreate") {
    var sc2: SparkContext = null
    SparkContext.clearActiveContext()//清除激活
    val conf = new SparkConf().setAppName("test").setMaster("local")

    sc = SparkContext.getOrCreate(conf)

    assert(sc.getConf.get("spark.app.name").equals("test"))
    sc2 = SparkContext.getOrCreate(new SparkConf().setAppName("test2").setMaster("local"))
    assert(sc2.getConf.get("spark.app.name").equals("test"))
    assert(sc === sc2)
    assert(sc eq sc2)

    // Try creating second context to confirm that it's still possible, if desired
    //尝试创建第二个上下文,以确认它仍然是可能的,如果需要的话
    sc2 = new SparkContext(new SparkConf().setAppName("test3").setMaster("local")
        .set("spark.driver.allowMultipleContexts", "true"))

    sc2.stop()
  }
  //BytesWritable正确的隐式转换
  test("BytesWritable implicit(隐式转换) conversion is correct") {
    // Regression test for SPARK-3121
    val bytesWritable = new BytesWritable()
    val inputArray = (1 to 10).map(_.toByte).toArray
    bytesWritable.set(inputArray, 0, 10)
    bytesWritable.set(inputArray, 0, 5)

    val converter = WritableConverter.bytesWritableConverter()
    val byteArray = converter.convert(bytesWritable)
    assert(byteArray.length === 5)

    bytesWritable.set(inputArray, 0, 0)
    val byteArray2 = converter.convert(bytesWritable)
    assert(byteArray2.length === 0)
  }
  //添加文件工作
  test("addFile works") {
    val dir = Utils.createTempDir()

    val file1 = File.createTempFile("someprefix1", "somesuffix1", dir)
    //返回抽象路径名的绝对路径名字符串。
    val absolutePath1 = file1.getAbsolutePath

    val file2 = File.createTempFile("someprefix2", "somesuffix2", dir)
    //相对路径
    val relativePath = file2.getParent + "/../" + file2.getParentFile.getName + "/" + file2.getName
    //绝对路径
    val absolutePath2 = file2.getAbsolutePath

    try {
      Files.write("somewords1", file1, UTF_8)
      Files.write("somewords2", file2, UTF_8)
      val length1 = file1.length()
      val length2 = file2.length()

      sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
      sc.addFile(file1.getAbsolutePath)
      sc.addFile(relativePath)
      sc.parallelize(Array(1), 1).map(x => {
        //SparkFiles获得Spark文件
        val gotten1 = new File(SparkFiles.get(file1.getName))//得到1
        val gotten2 = new File(SparkFiles.get(file2.getName))
        if (!gotten1.exists()) {
          throw new SparkException("file doesn't exist : " + absolutePath1)
        }
        if (!gotten2.exists()) {
          throw new SparkException("file doesn't exist : " + absolutePath2)
        }

        if (length1 != gotten1.length()) {
          throw new SparkException(
            s"file has different length $length1 than added file ${gotten1.length()} : " +
              absolutePath1)
        }
        if (length2 != gotten2.length()) {
          throw new SparkException(
            s"file has different length $length2 than added file ${gotten2.length()} : " +
              absolutePath2)
        }

        if (absolutePath1 == gotten1.getAbsolutePath) {
          throw new SparkException("file should have been copied :" + absolutePath1)
        }
        if (absolutePath2 == gotten2.getAbsolutePath) {
          throw new SparkException("file should have been copied : " + absolutePath2)
        }
        x
      }).count()
    } finally {
      sc.stop()
    }
  }
  //增加文件递归works
  test("addFile recursive works") {
    val pluto = Utils.createTempDir()
    //海王星
    val neptune = Utils.createTempDir(pluto.getAbsolutePath)
    //土星
    val saturn = Utils.createTempDir(neptune.getAbsolutePath)
    //外星人
    val alien1 = File.createTempFile("alien", "1", neptune)
    val alien2 = File.createTempFile("alien", "2", saturn)

    try {
      sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
      //海王星
      sc.addFile(neptune.getAbsolutePath, true)//递归添加子文件
      sc.parallelize(Array(1), 1).map(x => {
        val sep = File.separator//代表系统目录中的间隔符,说白了就是斜线
        if (!new File(SparkFiles.get(neptune.getName + sep + alien1.getName)).exists()) {
          throw new SparkException("can't access file under root added directory")
        }
        if (!new File(SparkFiles.get(neptune.getName + sep + saturn.getName + sep + alien2.getName))
            .exists()) {
          throw new SparkException("can't access file in nested directory")
        }
        if (new File(SparkFiles.get(pluto.getName + sep + neptune.getName + sep + alien1.getName))
            .exists()) {
          throw new SparkException("file exists that shouldn't")
        }
        x
      }).count()
    } finally {
      sc.stop()
    }
  }
  //增加递归文件,不能添加默认目录
  test("addFile recursive can't add directories by default") {
    val dir = Utils.createTempDir()

    try {
      sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
      intercept[SparkException] {
        sc.addFile(dir.getAbsolutePath)//递归创建文件
      }
    } finally {
      sc.stop()
    }
  }
  //取消工作组应该不会造成sparkcontext关机
  test("Cancelling job group should not cause SparkContext to shutdown (SPARK-6414)") {
    try {
      sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
      val future = sc.parallelize(Seq(0)).foreachAsync(_ => {Thread.sleep(1000L)})
      sc.cancelJobGroup("nonExistGroupId")
      Await.ready(future, Duration(2, TimeUnit.SECONDS))

      // In SPARK-6414, sc.cancelJobGroup will cause NullPointerException and cause
      // SparkContext to shutdown, so the following assertion will fail.
      //sparkcontext关闭,因此下面的断言将失败
      assert(sc.parallelize(1 to 10).count() == 10L)
    } finally {
      sc.stop()
    }
  }
  //逗号分隔的路径
  test("Comma separated paths for newAPIHadoopFile/wholeTextFiles/binaryFiles (SPARK-7155)") {
    // Regression test for SPARK-7155
    // dir1 and dir2 are used for wholeTextFiles and binaryFiles
    val dir1 = Utils.createTempDir() //创建临时目录
    val dir2 = Utils.createTempDir() //创建临时目录
  //C:\\Users\\liushuhua\\AppData\\Local\\Temp\\spark-dc06d21b-efce-42a8-8b2a-94262cb30b4e
    val dirpath1 = dir1.getAbsolutePath
    val dirpath2 = dir2.getAbsolutePath

    // file1 and file2 are placed inside dir1, they are also used for
    // file1和file2在 dir1目录,也用于文本文件,hadoop文件
    // textFile, hadoopFile, and newAPIHadoopFile
    // file3, file4 and file5 are placed inside dir2, they are used for
    //file3,file4和file5在 dir2目录,也用于文本文件,hadoop文件
    // textFile, hadoopFile, and newAPIHadoopFile as well
    val file1 = new File(dir1, "part-00000")
    val file2 = new File(dir1, "part-00001")
    val file3 = new File(dir2, "part-00000")
    val file4 = new File(dir2, "part-00001")
    val file5 = new File(dir2, "part-00002")

    val filepath1 = file1.getAbsolutePath
    val filepath2 = file2.getAbsolutePath
    val filepath3 = file3.getAbsolutePath
    val filepath4 = file4.getAbsolutePath
    val filepath5 = file5.getAbsolutePath


    try {
      // Create 5 text files.
      //创建文件内容
      Files.write("someline1 in file1\nsomeline2 in file1\nsomeline3 in file1", file1, UTF_8)
      Files.write("someline1 in file2\nsomeline2 in file2", file2, UTF_8)
      Files.write("someline1 in file3", file3, UTF_8)
      Files.write("someline1 in file4\nsomeline2 in file4", file4, UTF_8)
      Files.write("someline1 in file2\nsomeline2 in file5", file5, UTF_8)

      sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))

      // Test textFile, hadoopFile, and newAPIHadoopFile for file1 and file2
      assert(sc.textFile(filepath1 + "," + filepath2).count() == 5L)//增加普通文件
      assert(sc.hadoopFile(filepath1 + "," + filepath2,//Hadoop文件
        classOf[TextInputFormat], classOf[LongWritable], classOf[Text]).count() == 5L)
      assert(sc.newAPIHadoopFile(filepath1 + "," + filepath2,
        classOf[NewTextInputFormat], classOf[LongWritable], classOf[Text]).count() == 5L)

      // Test textFile, hadoopFile, and newAPIHadoopFile for file3, file4, and file5
      assert(sc.textFile(filepath3 + "," + filepath4 + "," + filepath5).count() == 5L)
      assert(sc.hadoopFile(filepath3 + "," + filepath4 + "," + filepath5,
               classOf[TextInputFormat], classOf[LongWritable], classOf[Text]).count() == 5L)
      assert(sc.newAPIHadoopFile(filepath3 + "," + filepath4 + "," + filepath5,
               classOf[NewTextInputFormat], classOf[LongWritable], classOf[Text]).count() == 5L)

      // Test wholeTextFiles, and binaryFiles for dir1 and dir2
      assert(sc.wholeTextFiles(dirpath1 + "," + dirpath2).count() == 5L)
      assert(sc.binaryFiles(dirpath1 + "," + dirpath2).count() == 5L)

    } finally {
      sc.stop()
    }
  }
  //调用多个sc.stop()不抛出任何异常
  test("calling multiple sc.stop() must not throw any exception") {
    noException should be thrownBy {
      sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
      val cnt = sc.parallelize(1 to 4).count()
      sc.cancelAllJobs()
      sc.stop()
      // call stop second time
      //第二次调用停止
      sc.stop()
    }
  }

  test("No exception when both num-executors and dynamic allocation set.") {
    noException should be thrownBy {
      sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local")
        .set("spark.dynamicAllocation.enabled", "true").set("spark.executor.instances", "6"))
      assert(sc.executorAllocationManager.isEmpty)
      assert(sc.getConf.getInt("spark.executor.instances", 0) === 6)
    }
  }
}
