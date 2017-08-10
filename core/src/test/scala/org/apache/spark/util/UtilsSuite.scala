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

package org.apache.spark.util

import java.io.{File, ByteArrayOutputStream, ByteArrayInputStream, FileOutputStream}
import java.lang.{Double => JDouble, Float => JFloat}
import java.net.{BindException, ServerSocket, URI}
import java.nio.{ByteBuffer, ByteOrder}
import java.text.DecimalFormatSymbols
import java.util.concurrent.TimeUnit
import java.util.Locale

import scala.collection.mutable.ListBuffer
import scala.util.Random

import com.google.common.base.Charsets.UTF_8
import com.google.common.io.Files

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.network.util.ByteUnit
import org.apache.spark.{Logging, SparkFunSuite}
import org.apache.spark.SparkConf

class UtilsSuite extends SparkFunSuite with ResetSystemProperties with Logging {

  test("timeConversion") {//时间转换
    // Test -1 Seconds秒
    assert(Utils.timeStringAsSeconds("-1") === -1)

    // Test zero 测试零
    assert(Utils.timeStringAsSeconds("0") === 0)

    assert(Utils.timeStringAsSeconds("1") === 1)
    //1秒
    assert(Utils.timeStringAsSeconds("1s") === 1)
    assert(Utils.timeStringAsSeconds("1000ms") === 1)
    assert(Utils.timeStringAsSeconds("1000000us") === 1)
    //1分
    assert(Utils.timeStringAsSeconds("1m") === TimeUnit.MINUTES.toSeconds(1))
    //1分
    assert(Utils.timeStringAsSeconds("1min") === TimeUnit.MINUTES.toSeconds(1))
    //1小时
    assert(Utils.timeStringAsSeconds("1h") === TimeUnit.HOURS.toSeconds(1))
    //1天
    assert(Utils.timeStringAsSeconds("1d") === TimeUnit.DAYS.toSeconds(1))
    //
    assert(Utils.timeStringAsMs("1") === 1)//秒
    assert(Utils.timeStringAsMs("1ms") === 1)//秒
    assert(Utils.timeStringAsMs("1000us") === 1)//
    assert(Utils.timeStringAsMs("1s") === TimeUnit.SECONDS.toMillis(1))//秒
    assert(Utils.timeStringAsMs("1m") === TimeUnit.MINUTES.toMillis(1))//分
    assert(Utils.timeStringAsMs("1min") === TimeUnit.MINUTES.toMillis(1))//分
    assert(Utils.timeStringAsMs("1h") === TimeUnit.HOURS.toMillis(1))//小时
    assert(Utils.timeStringAsMs("1d") === TimeUnit.DAYS.toMillis(1))//天

    // Test invalid strings,测试无效的字符串
    intercept[NumberFormatException] {
      Utils.timeStringAsMs("600l")
    }

    intercept[NumberFormatException] {
      Utils.timeStringAsMs("This breaks 600s")
    }

    intercept[NumberFormatException] {
      Utils.timeStringAsMs("This breaks 600ds")
    }

    intercept[NumberFormatException] {
      Utils.timeStringAsMs("600s This breaks")
    }

    intercept[NumberFormatException] {
      Utils.timeStringAsMs("This 123s breaks")
    }
  }

  test("Test byteString conversion") {//字节字符串转换
    // Test zero
    assert(Utils.byteStringAsBytes("0") === 0)
    //byteString
    assert(Utils.byteStringAsGb("1") === 1)//G
    assert(Utils.byteStringAsGb("1g") === 1)
    assert(Utils.byteStringAsGb("1023m") === 0)//
    assert(Utils.byteStringAsGb("1024m") === 1)//G
    assert(Utils.byteStringAsGb("1048575k") === 0)
    assert(Utils.byteStringAsGb("1048576k") === 1)
    assert(Utils.byteStringAsGb("1k") === 0)
    assert(Utils.byteStringAsGb("1t") === ByteUnit.TiB.toGiB(1))
    assert(Utils.byteStringAsGb("1p") === ByteUnit.PiB.toGiB(1))

    assert(Utils.byteStringAsMb("1") === 1)
    assert(Utils.byteStringAsMb("1m") === 1)
    assert(Utils.byteStringAsMb("1048575b") === 0)
    assert(Utils.byteStringAsMb("1048576b") === 1)
    assert(Utils.byteStringAsMb("1023k") === 0)
    assert(Utils.byteStringAsMb("1024k") === 1)
    assert(Utils.byteStringAsMb("3645k") === 3)
    assert(Utils.byteStringAsMb("1024gb") === 1048576)
    assert(Utils.byteStringAsMb("1g") === ByteUnit.GiB.toMiB(1))
    assert(Utils.byteStringAsMb("1t") === ByteUnit.TiB.toMiB(1))
    assert(Utils.byteStringAsMb("1p") === ByteUnit.PiB.toMiB(1))

    assert(Utils.byteStringAsKb("1") === 1)
    assert(Utils.byteStringAsKb("1k") === 1)
    assert(Utils.byteStringAsKb("1m") === ByteUnit.MiB.toKiB(1))
    assert(Utils.byteStringAsKb("1g") === ByteUnit.GiB.toKiB(1))
    assert(Utils.byteStringAsKb("1t") === ByteUnit.TiB.toKiB(1))
    assert(Utils.byteStringAsKb("1p") === ByteUnit.PiB.toKiB(1))

    assert(Utils.byteStringAsBytes("1") === 1)
    assert(Utils.byteStringAsBytes("1k") === ByteUnit.KiB.toBytes(1))
    assert(Utils.byteStringAsBytes("1m") === ByteUnit.MiB.toBytes(1))
    assert(Utils.byteStringAsBytes("1g") === ByteUnit.GiB.toBytes(1))
    assert(Utils.byteStringAsBytes("1t") === ByteUnit.TiB.toBytes(1))
    assert(Utils.byteStringAsBytes("1p") === ByteUnit.PiB.toBytes(1))

    // Overflow handling, 1073741824p exceeds Long.MAX_VALUE if converted straight to Bytes
    //溢出处理,如果1073741824p超过long.max_value值直接转换为字节
    // This demonstrates that we can have e.g 1024^3 PB without overflowing.
    assert(Utils.byteStringAsGb("1073741824p") === ByteUnit.PiB.toGiB(1073741824))
    assert(Utils.byteStringAsMb("1073741824p") === ByteUnit.PiB.toMiB(1073741824))

    // Run this to confirm it doesn't throw an exception
    //运行此确认它不会抛出异常
    assert(Utils.byteStringAsBytes("9223372036854775807") === 9223372036854775807L)
    assert(ByteUnit.PiB.toPiB(9223372036854775807L) === 9223372036854775807L)

    // Test overflow exception 测试溢出异常
    intercept[IllegalArgumentException] {
      // This value exceeds Long.MAX when converted to bytes
      //该值超过long.max当转换为字节
      Utils.byteStringAsBytes("9223372036854775808")
    }

    // Test overflow exception 测试溢出异常
    intercept[IllegalArgumentException] {
      // This value exceeds Long.MAX when converted to TB
      //该值超过long.max当转换为TB
      ByteUnit.PiB.toTiB(9223372036854775807L)
    }

    // Test fractional string 测试小数字符串
    intercept[NumberFormatException] {
      Utils.byteStringAsMb("0.064")
    }

    // Test fractional string 测试小数字符串
    intercept[NumberFormatException] {
      Utils.byteStringAsMb("0.064m")
    }

    // Test invalid strings 无效的字符串
    intercept[NumberFormatException] {
      Utils.byteStringAsBytes("500ub")
    }

    // Test invalid strings 无效的字符串
    intercept[NumberFormatException] {
      Utils.byteStringAsBytes("This breaks 600b")
    }

    intercept[NumberFormatException] {
      Utils.byteStringAsBytes("This breaks 600")
    }

    intercept[NumberFormatException] {
      Utils.byteStringAsBytes("600gb This breaks")
    }

    intercept[NumberFormatException] {
      Utils.byteStringAsBytes("This 123mb breaks")
    }
  }

  test("bytesToString") {//字节到字符串转换
    //bytes转换字符串KB
    assert(Utils.bytesToString(10) === "10.0 B")
    assert(Utils.bytesToString(1500) === "1500.0 B")
    assert(Utils.bytesToString(2000000) === "1953.1 KB")
    assert(Utils.bytesToString(2097152) === "2.0 MB")
    assert(Utils.bytesToString(2306867) === "2.2 MB")
    assert(Utils.bytesToString(5368709120L) === "5.0 GB")
    assert(Utils.bytesToString(5L * 1024L * 1024L * 1024L * 1024L) === "5.0 TB")
  }

  test("copyStream") {//复制流
    // input array initialization
    // 输入数组初始化
    val bytes = Array.ofDim[Byte](9000)
    Random.nextBytes(bytes)

    val os = new ByteArrayOutputStream()
    //ByteArray流复制,输入流复制到输出流
    Utils.copyStream(new ByteArrayInputStream(bytes), os)

    assert(os.toByteArray.toList.equals(bytes.toList))
  }

  test("memoryStringToMb") {//内存字符串转换Mb
    assert(Utils.memoryStringToMb("1") === 0)
    assert(Utils.memoryStringToMb("1048575") === 0)
    assert(Utils.memoryStringToMb("3145728") === 3)

    assert(Utils.memoryStringToMb("1024k") === 1)
    assert(Utils.memoryStringToMb("5000k") === 4)
    assert(Utils.memoryStringToMb("4024k") === Utils.memoryStringToMb("4024K"))

    assert(Utils.memoryStringToMb("1024m") === 1024)
    assert(Utils.memoryStringToMb("5000m") === 5000)
    assert(Utils.memoryStringToMb("4024m") === Utils.memoryStringToMb("4024M"))

    assert(Utils.memoryStringToMb("2g") === 2048)
    assert(Utils.memoryStringToMb("3g") === Utils.memoryStringToMb("3G"))

    assert(Utils.memoryStringToMb("2t") === 2097152)
    assert(Utils.memoryStringToMb("3t") === Utils.memoryStringToMb("3T"))
  }

  test("splitCommandString") {//字符串分割命令
    //字符串分隔成序列
    assert(Utils.splitCommandString("") === Seq())
    assert(Utils.splitCommandString("a") === Seq("a"))
    assert(Utils.splitCommandString("aaa") === Seq("aaa"))
    assert(Utils.splitCommandString("a b c") === Seq("a", "b", "c"))
    assert(Utils.splitCommandString("  a   b\t c ") === Seq("a", "b", "c"))
    assert(Utils.splitCommandString("a 'b c'") === Seq("a", "b c"))
    assert(Utils.splitCommandString("a 'b c' d") === Seq("a", "b c", "d"))
    assert(Utils.splitCommandString("'b c'") === Seq("b c"))
    assert(Utils.splitCommandString("a \"b c\"") === Seq("a", "b c"))
    assert(Utils.splitCommandString("a \"b c\" d") === Seq("a", "b c", "d"))
    assert(Utils.splitCommandString("\"b c\"") === Seq("b c"))
    assert(Utils.splitCommandString("a 'b\" c' \"d' e\"") === Seq("a", "b\" c", "d' e"))
    assert(Utils.splitCommandString("a\t'b\nc'\nd") === Seq("a", "b\nc", "d"))
    assert(Utils.splitCommandString("a \"b\\\\c\"") === Seq("a", "b\\c"))
    assert(Utils.splitCommandString("a \"b\\\"c\"") === Seq("a", "b\"c"))
    assert(Utils.splitCommandString("a 'b\\\"c'") === Seq("a", "b\\\"c"))
    assert(Utils.splitCommandString("'a'b") === Seq("ab"))
    assert(Utils.splitCommandString("'a''b'") === Seq("ab"))
    assert(Utils.splitCommandString("\"a\"b") === Seq("ab"))
    assert(Utils.splitCommandString("\"a\"\"b\"") === Seq("ab"))
    assert(Utils.splitCommandString("''") === Seq(""))
    assert(Utils.splitCommandString("\"\"") === Seq(""))
  }

  test("string formatting of time durations") {//时间持续字符串格式化
    val second = 1000
    val minute = second * 60
    val hour = minute * 60
    def str: (Long) => String = Utils.msDurationToString(_)
    //
    val sep = new DecimalFormatSymbols(Locale.getDefault()).getDecimalSeparator()

    assert(str(123) === "123 ms")//秒
    assert(str(second) === "1" + sep + "0 s")
    assert(str(second + 462) === "1" + sep + "5 s")
    assert(str(hour) === "1" + sep + "00 h")
    assert(str(minute) === "1" + sep + "0 m")
    assert(str(minute + 4 * second + 34) === "1" + sep + "1 m")
    assert(str(10 * hour + minute + 4 * second) === "10" + sep + "02 h")
    assert(str(10 * hour + 59 * minute + 59 * second + 999) === "11" + sep + "00 h")
  }

  test("reading offset bytes of a file") {//读取文件的偏移量字节数
    val tmpDir2 = Utils.createTempDir()//
    val f1Path = tmpDir2 + "/f1"
    val f1 = new FileOutputStream(f1Path)
    f1.write("1\n2\n3\n4\n5\n6\n7\n8\n9\n".getBytes(UTF_8))
    f1.close()

    // Read first few bytes,读前几字节
    assert(Utils.offsetBytes(f1Path, 0, 5) === "1\n2\n3")

    // Read some middle bytes 读一些中间字节
    assert(Utils.offsetBytes(f1Path, 4, 11) === "3\n4\n5\n6")

    // Read last few bytes 读最后几个字节
    assert(Utils.offsetBytes(f1Path, 12, 18) === "7\n8\n9\n")

    // Read some nonexistent bytes in the beginning
    //开始读一些不存在的字节
    assert(Utils.offsetBytes(f1Path, -5, 5) === "1\n2\n3")

    // Read some nonexistent bytes at the end
    //读取结束时的一些不存在的字节数
    assert(Utils.offsetBytes(f1Path, 12, 22) === "7\n8\n9\n")

    // Read some nonexistent bytes on both ends
    //读一些不存在的字节两端
    assert(Utils.offsetBytes(f1Path, -3, 25) === "1\n2\n3\n4\n5\n6\n7\n8\n9\n")

    Utils.deleteRecursively(tmpDir2)
  }

  test("reading offset bytes across multiple files") {//读取多个文件的偏移量字节数
    val tmpDir = Utils.createTempDir()
    val files = (1 to 3).map(i => new File(tmpDir, i.toString))//files:IndexedSeq
    Files.write("0123456789", files(0), UTF_8)
    Files.write("abcdefghij", files(1), UTF_8)
    Files.write("ABCDEFGHIJ", files(2), UTF_8)

    // Read first few bytes in the 1st file
    //在第一文件中读取前几个字节
    assert(Utils.offsetBytes(files, 0, 5) === "01234")

    // Read bytes within the 1st file
    //读取第一个文件中的字节
    assert(Utils.offsetBytes(files, 5, 8) === "567")

    // Read bytes across 1st and 2nd file
    //读取第一和第二文件的字节数
    assert(Utils.offsetBytes(files, 8, 18) === "89abcdefgh")

    // Read bytes across 1st, 2nd and 3rd file
    //读取第一个,第二个和第三个文件的字节数
    assert(Utils.offsetBytes(files, 5, 24) === "56789abcdefghijABCD")

    // Read some nonexistent bytes in the beginning
    //开始读一些不存在的字节
    assert(Utils.offsetBytes(files, -5, 18) === "0123456789abcdefgh")

    // Read some nonexistent bytes at the end
    //读取结束时的一些不存在的字节数
    assert(Utils.offsetBytes(files, 18, 35) === "ijABCDEFGHIJ")

    // Read some nonexistent bytes on both ends
    //读一些不存在的字节两端
    assert(Utils.offsetBytes(files, -5, 35) === "0123456789abcdefghijABCDEFGHIJ")
   //递归删除
    Utils.deleteRecursively(tmpDir)
  }

  test("deserialize long value") {//反序列化长值
    val testval : Long = 9730889947L
    val bbuf = ByteBuffer.allocate(8)
    assert(bbuf.hasArray)
    bbuf.order(ByteOrder.BIG_ENDIAN)
    bbuf.putLong(testval)
    assert(bbuf.array.length === 8)
    assert(Utils.deserializeLongValue(bbuf.array) === testval)
  }

  test("get iterator size") {//获得迭代器大小
    val empty = Seq[Int]()
    assert(Utils.getIteratorSize(empty.toIterator) === 0L)
    val iterator = Iterator.range(0, 5)
    assert(Utils.getIteratorSize(iterator) === 5L)
  }
  //目录包含新文件的文件
  test("doesDirectoryContainFilesNewerThan") {
    // create some temporary directories and files
    //创建一些临时目录和文件
    val parent: File = Utils.createTempDir()
    // The parent directory has two child directories
    //父目录有两个子目录
    val child1: File = Utils.createTempDir(parent.getCanonicalPath)
    val child2: File = Utils.createTempDir(parent.getCanonicalPath)
    val child3: File = Utils.createTempDir(child1.getCanonicalPath)
    // set the last modified time of child1 to 30 secs old
    //最后修改时间为30秒
    child1.setLastModified(System.currentTimeMillis() - (1000 * 30))

    // although child1 is old, child2 is still new so return true
    //虽然child1是老的,child2仍然是新的,所以返回true
    assert(Utils.doesDirectoryContainAnyNewFiles(parent, 5))

    child2.setLastModified(System.currentTimeMillis - (1000 * 30))
    assert(Utils.doesDirectoryContainAnyNewFiles(parent, 5))

    parent.setLastModified(System.currentTimeMillis - (1000 * 30))
    // although parent and its immediate children are new, child3 is still old
    // we expect a full recursive search for new files.
    assert(Utils.doesDirectoryContainAnyNewFiles(parent, 5))

    child3.setLastModified(System.currentTimeMillis - (1000 * 30))
    assert(!Utils.doesDirectoryContainAnyNewFiles(parent, 5))
  }

  test("resolveURI") {//解析URI
    def assertResolves(before: String, after: String): Unit = {
      // This should test only single paths
      //测试单一路径
      assume(before.split(",").length === 1)
      // Repeated invocations of resolveURI should yield the same result
      //重复调用ResolveUri应产生相同的结果
      def resolve(uri: String): String = Utils.resolveURI(uri).toString
      println(before+"====="+resolve(after))
      assert(resolve(after) === after)
      assert(resolve(resolve(after)) === after)
      assert(resolve(resolve(resolve(after))) === after)
      // Also test resolveURIs with single paths
      
      assert(new URI(Utils.resolveURIs(before)) === new URI(after))
      assert(new URI(Utils.resolveURIs(after)) === new URI(after))
    }
    //System.getProperty("user.dir")是取得当前工作目录
    val rawCwd = System.getProperty("user.dir")
    val cwd = if (Utils.isWindows) s"/$rawCwd".replace("\\", "/") else rawCwd
    assertResolves("hdfs:/root/spark.jar", "hdfs:/root/spark.jar")
    assertResolves("hdfs:///root/spark.jar#app.jar", "hdfs:/root/spark.jar#app.jar")
    assertResolves("spark.jar", s"file:$cwd/spark.jar")
    assertResolves("spark.jar#app.jar", s"file:$cwd/spark.jar#app.jar")
    assertResolves("path to/file.txt", s"file:$cwd/path%20to/file.txt")
    if (Utils.isWindows) {
      assertResolves("C:\\path\\to\\file.txt", "file:/C:/path/to/file.txt")
      assertResolves("C:\\path to\\file.txt", "file:/C:/path%20to/file.txt")
    }
    assertResolves("file:/C:/path/to/file.txt", "file:/C:/path/to/file.txt")
    assertResolves("file:///C:/path/to/file.txt", "file:/C:/path/to/file.txt")
    assertResolves("file:/C:/file.txt#alias.txt", "file:/C:/file.txt#alias.txt")
    assertResolves("file:foo", s"file:foo")
    assertResolves("file:foo:baby", s"file:foo:baby")
  }

  test("resolveURIs with multiple paths") {//解析多个路径的URL
    def assertResolves(before: String, after: String): Unit = {
      assume(before.split(",").length > 1)
      println(before+"====="+Utils.resolveURIs(before))
      assert(Utils.resolveURIs(before) === after)
      assert(Utils.resolveURIs(after) === after)
      // Repeated invocations of resolveURIs should yield the same result
      //resolveURI的重复调用应该产生相同的结果
      def resolve(uri: String): String = Utils.resolveURIs(uri)
      println(before+"====="+resolve(before))
      assert(resolve(after) === after)
      assert(resolve(resolve(after)) === after)
      assert(resolve(resolve(resolve(after))) === after)
    }
    val rawCwd = System.getProperty("user.dir")
    val cwd = if (Utils.isWindows) s"/$rawCwd".replace("\\", "/") else rawCwd
    assertResolves("jar1,jar2", s"file:$cwd/jar1,file:$cwd/jar2")
    assertResolves("file:/jar1,file:/jar2", "file:/jar1,file:/jar2")
    assertResolves("hdfs:/jar1,file:/jar2,jar3", s"hdfs:/jar1,file:/jar2,file:$cwd/jar3")
    assertResolves("hdfs:/jar1,file:/jar2,jar3,jar4#jar5,path to/jar6",
      s"hdfs:/jar1,file:/jar2,file:$cwd/jar3,file:$cwd/jar4#jar5,file:$cwd/path%20to/jar6")
    if (Utils.isWindows) {
      assertResolves("""hdfs:/jar1,file:/jar2,jar3,C:\pi.py#py.pi,C:\path to\jar4""",
        s"hdfs:/jar1,file:/jar2,file:$cwd/jar3,file:/C:/pi.py#py.pi,file:/C:/path%20to/jar4")
    }
  }

  test("nonLocalPaths") {
    //非本地路径
    assert(Utils.nonLocalPaths("spark.jar") === Array.empty)//非本地路径
    assert(Utils.nonLocalPaths("file:/spark.jar") === Array.empty)
    assert(Utils.nonLocalPaths("file:///spark.jar") === Array.empty)
    assert(Utils.nonLocalPaths("local:/spark.jar") === Array.empty)
    assert(Utils.nonLocalPaths("local:///spark.jar") === Array.empty)
    assert(Utils.nonLocalPaths("hdfs:/spark.jar") === Array("hdfs:/spark.jar"))
    assert(Utils.nonLocalPaths("hdfs:///spark.jar") === Array("hdfs:///spark.jar"))
    assert(Utils.nonLocalPaths("file:/spark.jar,local:/smart.jar,family.py") === Array.empty)
    assert(Utils.nonLocalPaths("local:/spark.jar,file:/smart.jar,family.py") === Array.empty)
    assert(Utils.nonLocalPaths("hdfs:/spark.jar,s3:/smart.jar") ===
      Array("hdfs:/spark.jar", "s3:/smart.jar"))
    assert(Utils.nonLocalPaths("hdfs:/spark.jar,path to/a.jar,s3:/smart.jar") ===
      Array("hdfs:/spark.jar", "s3:/smart.jar"))
    assert(Utils.nonLocalPaths("hdfs:/spark.jar,s3:/smart.jar,local.py,file:/hello/pi.py") ===
      Array("hdfs:/spark.jar", "s3:/smart.jar"))
    assert(Utils.nonLocalPaths("local.py,hdfs:/spark.jar,file:/hello/pi.py,s3:/smart.jar") ===
      Array("hdfs:/spark.jar", "s3:/smart.jar"))

    // Test Windows paths 测试Windows路径
    assert(Utils.nonLocalPaths("C:/some/path.jar", testWindows = true) === Array.empty)
    assert(Utils.nonLocalPaths("file:/C:/some/path.jar", testWindows = true) === Array.empty)
    assert(Utils.nonLocalPaths("file:///C:/some/path.jar", testWindows = true) === Array.empty)
    assert(Utils.nonLocalPaths("local:/C:/some/path.jar", testWindows = true) === Array.empty)
    assert(Utils.nonLocalPaths("local:///C:/some/path.jar", testWindows = true) === Array.empty)
    assert(Utils.nonLocalPaths("hdfs:/a.jar,C:/my.jar,s3:/another.jar", testWindows = true) ===
      Array("hdfs:/a.jar", "s3:/another.jar"))
    assert(Utils.nonLocalPaths("D:/your.jar,hdfs:/a.jar,s3:/another.jar", testWindows = true) ===
      Array("hdfs:/a.jar", "s3:/another.jar"))
    assert(Utils.nonLocalPaths("hdfs:/a.jar,s3:/another.jar,e:/our.jar", testWindows = true) ===
      Array("hdfs:/a.jar", "s3:/another.jar"))
  }

  test("isBindCollision") {//绑定冲突
    // Negatives 否定
    assert(!Utils.isBindCollision(null))
    assert(!Utils.isBindCollision(new Exception))
    assert(!Utils.isBindCollision(new Exception(new Exception)))
    assert(!Utils.isBindCollision(new Exception(new BindException)))

    // Positives
    val be = new BindException("Random Message")
    val be1 = new Exception(new BindException("Random Message"))
    val be2 = new Exception(new Exception(new BindException("Random Message")))
    assert(Utils.isBindCollision(be))
    assert(Utils.isBindCollision(be1))
    assert(Utils.isBindCollision(be2))

    // Actual bind exception 实际绑定的异常
    var server1: ServerSocket = null
    var server2: ServerSocket = null
    try {
      server1 = new java.net.ServerSocket(0)
      server2 = new java.net.ServerSocket(server1.getLocalPort)
    } catch {
      case e: Exception =>
        assert(e.isInstanceOf[java.net.BindException])
        assert(Utils.isBindCollision(e))
    } finally {
      Option(server1).foreach(_.close())
      Option(server2).foreach(_.close())
    }
  }

  // Test for using the util function to change our log levels.\
  //测试使用工具函数来改变日志级别
  test("log4j log level change") {
    //log4j日志等级变更
    val current = org.apache.log4j.Logger.getRootLogger().getLevel()//获得当前日志等级
    try {
      Utils.setLogLevel(org.apache.log4j.Level.ALL)
      assert(log.isInfoEnabled())
      Utils.setLogLevel(org.apache.log4j.Level.ERROR)
      assert(!log.isInfoEnabled())
      assert(log.isErrorEnabled())
    } finally {
      // Best effort at undoing changes this test made.
      //改变这个测试失败
      Utils.setLogLevel(current)
    }
  }

  test("deleteRecursively") { //递归删除   
    val tempDir1 = Utils.createTempDir()//创建临时目录
    assert(tempDir1.exists())
    Utils.deleteRecursively(tempDir1)
    assert(!tempDir1.exists())

    val tempDir2 = Utils.createTempDir()
    val sourceFile1 = new File(tempDir2, "foo.txt")
    /**
     * touch是用来修改指定的文件的访问和修改时间属性,如果指定的文件不存在,
     * 将建立一个新的空文件,并以当前的时间来设置文件的访问和修改时间
     */
    Files.touch(sourceFile1)
    assert(sourceFile1.exists())
    Utils.deleteRecursively(sourceFile1)
    assert(!sourceFile1.exists())

    val tempDir3 = new File(tempDir2, "subdir")
    assert(tempDir3.mkdir())
    val sourceFile2 = new File(tempDir3, "bar.txt")
    Files.touch(sourceFile2)
    assert(sourceFile2.exists())
    Utils.deleteRecursively(tempDir2)
    assert(!tempDir2.exists())
    assert(!tempDir3.exists())
    assert(!sourceFile2.exists())
  }

  test("loading properties from file") {//从文件加载属性   
    val tmpDir = Utils.createTempDir()
    val outFile = File.createTempFile("test-load-spark-properties", "test", tmpDir)
    try {
      System.setProperty("spark.test.fileNameLoadB", "2")
      Files.write("spark.test.fileNameLoadA true\n" +
        "spark.test.fileNameLoadB 1\n", outFile, UTF_8)
      //加一个properties文件
      val properties = Utils.getPropertiesFromFile(outFile.getAbsolutePath)
      //System.getenv()和System.getProperties()的区别
      //System.getenv() 返回系统环境变量值 设置系统环境变量：当前登录用户主目录下的".bashrc"文件中可以设置系统环境变量
      //System.getProperties() 返回Java进程变量值 通过命令行参数的"-D"选项
      properties
        .filter { case (k, v) => k.startsWith("spark.")}//操作配制文件
        .foreach { case (k, v) => sys.props.getOrElseUpdate(k, v)}
      val sparkConf = new SparkConf
      assert(sparkConf.getBoolean("spark.test.fileNameLoadA", false) === true)
      assert(sparkConf.getInt("spark.test.fileNameLoadB", 1) === 2)
    } finally {
      Utils.deleteRecursively(tmpDir)
    }
  }
  //时间与准备
  test("timeIt with prepare") {
    var cnt = 0
    val prepare = () => {
      cnt += 1
      Thread.sleep(1000)
    }
    val time = Utils.timeIt(2)({}, Some(prepare))
    require(cnt === 2, "prepare should be called twice")
    require(time < 500, "preparation time should not count")
  }

  test("fetch hcfs dir") {//获取hdfs目录
    val tempDir = Utils.createTempDir()
    val sourceDir = new File(tempDir, "source-dir")
    val innerSourceDir = Utils.createTempDir(root = sourceDir.getPath)
    val sourceFile = File.createTempFile("someprefix", "somesuffix", innerSourceDir)
    val targetDir = new File(tempDir, "target-dir")
    Files.write("some text", sourceFile, UTF_8)

    val path =
      if (Utils.isWindows) {
        new Path("file:/" + sourceDir.getAbsolutePath.replace("\\", "/"))
      } else {
        new Path("file://" + sourceDir.getAbsolutePath)
      }
    val conf = new Configuration()
    val fs = Utils.getHadoopFileSystem(path.toString, conf)

    assert(!targetDir.isDirectory())
    Utils.fetchHcfsFile(path, targetDir, fs, new SparkConf(), conf, false)
    assert(targetDir.isDirectory())

    // Copy again to make sure it doesn't error if the dir already exists.
    //如果目录已经存在，请再次复制以确保它不会出错
    Utils.fetchHcfsFile(path, targetDir, fs, new SparkConf(), conf, false)

    val destDir = new File(targetDir, sourceDir.getName())
    assert(destDir.isDirectory())

    val destInnerDir = new File(destDir, innerSourceDir.getName)
    assert(destInnerDir.isDirectory())

    val destInnerFile = new File(destInnerDir, sourceFile.getName)
    assert(destInnerFile.isFile())

    val filePath =
      if (Utils.isWindows) {
        new Path("file:/" + sourceFile.getAbsolutePath.replace("\\", "/"))
      } else {
        new Path("file://" + sourceFile.getAbsolutePath)
      }
    val testFileDir = new File(tempDir, "test-filename")
    val testFileName = "testFName"
    val testFilefs = Utils.getHadoopFileSystem(filePath.toString, conf)
    Utils.fetchHcfsFile(filePath, testFileDir, testFilefs, new SparkConf(),
                        conf, false, Some(testFileName))
    val newFileName = new File(testFileDir, testFileName)
    assert(newFileName.isFile())
  }

  test("shutdown hook manager") {//管理关闭钩子回调 
    val manager = new SparkShutdownHookManager()
    val output = new ListBuffer[Int]()

    val hook1 = manager.add(1, () => output += 1)
    manager.add(3, () => output += 3)
    manager.add(2, () => output += 2)
    manager.add(4, () => output += 4)
    manager.remove(hook1)//删除1

    manager.runAll()
    assert(output.toList === List(4, 3, 2))
  }

  test("isInDirectory") {//判断在目录
  //System.getenv()和System.getProperties()的区别
  //System.getenv() 返回系统环境变量值 设置系统环境变量：当前登录用户主目录下的".bashrc"文件中可以设置系统环境变量
  //System.getProperties() 返回Java进程变量值 通过命令行参数的"-D"选项
    val tmpDir = new File(sys.props("java.io.tmpdir"))
    val parentDir = new File(tmpDir, "parent-dir")
    val childDir1 = new File(parentDir, "child-dir-1")
    val childDir1b = new File(parentDir, "child-dir-1b")
    val childFile1 = new File(parentDir, "child-file-1.txt")
    val childDir2 = new File(childDir1, "child-dir-2")
    val childDir2b = new File(childDir1, "child-dir-2b")
    val childFile2 = new File(childDir1, "child-file-2.txt")
    val childFile3 = new File(childDir2, "child-file-3.txt")
    val nullFile: File = null
    parentDir.mkdir()
    childDir1.mkdir()
    childDir1b.mkdir()
    childDir2.mkdir()
    childDir2b.mkdir()
    childFile1.createNewFile()
    childFile2.createNewFile()
    childFile3.createNewFile()

    // Identity 一致
    assert(Utils.isInDirectory(parentDir, parentDir))
    assert(Utils.isInDirectory(childDir1, childDir1))
    assert(Utils.isInDirectory(childDir2, childDir2))

    // Valid ancestor-descendant pairs
    //验证有效祖先-后代对
    assert(Utils.isInDirectory(parentDir, childDir1))
    assert(Utils.isInDirectory(parentDir, childFile1))
    assert(Utils.isInDirectory(parentDir, childDir2))
    assert(Utils.isInDirectory(parentDir, childFile2))
    assert(Utils.isInDirectory(parentDir, childFile3))
    assert(Utils.isInDirectory(childDir1, childDir2))
    assert(Utils.isInDirectory(childDir1, childFile2))
    assert(Utils.isInDirectory(childDir1, childFile3))
    assert(Utils.isInDirectory(childDir2, childFile3))

    // Inverted ancestor-descendant pairs should fail
    //失败祖先-后代对
    assert(!Utils.isInDirectory(childDir1, parentDir))
    assert(!Utils.isInDirectory(childDir2, parentDir))
    assert(!Utils.isInDirectory(childDir2, childDir1))
    assert(!Utils.isInDirectory(childFile1, parentDir))
    assert(!Utils.isInDirectory(childFile2, parentDir))
    assert(!Utils.isInDirectory(childFile3, parentDir))
    assert(!Utils.isInDirectory(childFile2, childDir1))
    assert(!Utils.isInDirectory(childFile3, childDir1))
    assert(!Utils.isInDirectory(childFile3, childDir2))

    // Non-existent files or directories should fail
    //不存在的文件或目录应该失败
    assert(!Utils.isInDirectory(parentDir, new File(parentDir, "one.txt")))
    assert(!Utils.isInDirectory(parentDir, new File(parentDir, "one/two.txt")))
    assert(!Utils.isInDirectory(parentDir, new File(parentDir, "one/two/three.txt")))

    // Siblings should fail
    assert(!Utils.isInDirectory(childDir1, childDir1b))
    assert(!Utils.isInDirectory(childDir1, childFile1))
    assert(!Utils.isInDirectory(childDir2, childDir2b))
    assert(!Utils.isInDirectory(childDir2, childFile2))

    // Null files should fail without throwing NPE
    //空文件应该没有抛出NPE失败
    assert(!Utils.isInDirectory(parentDir, nullFile))
    assert(!Utils.isInDirectory(childFile3, nullFile))
    assert(!Utils.isInDirectory(nullFile, parentDir))
    assert(!Utils.isInDirectory(nullFile, childFile3))
  }

  test("circular buffer") {//循环缓冲区
    val buffer = new CircularBuffer(25)
    val stream = new java.io.PrintStream(buffer, true, "UTF-8")

    // scalastyle:off println
    stream.println("test circular test circular test circular test circular test circular")
    // scalastyle:on println
    assert(buffer.toString === "t circular test circular\n")
  }

  test("nanSafeCompareDoubles") {
    def shouldMatchDefaultOrder(a: Double, b: Double): Unit = {
      assert(Utils.nanSafeCompareDoubles(a, b) === JDouble.compare(a, b))
      assert(Utils.nanSafeCompareDoubles(b, a) === JDouble.compare(b, a))
    }
    shouldMatchDefaultOrder(0d, 0d)
    shouldMatchDefaultOrder(0d, 1d)
    shouldMatchDefaultOrder(Double.MinValue, Double.MaxValue)
    assert(Utils.nanSafeCompareDoubles(Double.NaN, Double.NaN) === 0)
    assert(Utils.nanSafeCompareDoubles(Double.NaN, Double.PositiveInfinity) === 1)
    assert(Utils.nanSafeCompareDoubles(Double.NaN, Double.NegativeInfinity) === 1)
    assert(Utils.nanSafeCompareDoubles(Double.PositiveInfinity, Double.NaN) === -1)
    assert(Utils.nanSafeCompareDoubles(Double.NegativeInfinity, Double.NaN) === -1)
  }

  test("nanSafeCompareFloats") {
    def shouldMatchDefaultOrder(a: Float, b: Float): Unit = {
      assert(Utils.nanSafeCompareFloats(a, b) === JFloat.compare(a, b))
      assert(Utils.nanSafeCompareFloats(b, a) === JFloat.compare(b, a))
    }
    shouldMatchDefaultOrder(0f, 0f)
    shouldMatchDefaultOrder(1f, 1f)
    shouldMatchDefaultOrder(Float.MinValue, Float.MaxValue)
    assert(Utils.nanSafeCompareFloats(Float.NaN, Float.NaN) === 0)
    assert(Utils.nanSafeCompareFloats(Float.NaN, Float.PositiveInfinity) === 1)
    assert(Utils.nanSafeCompareFloats(Float.NaN, Float.NegativeInfinity) === 1)
    assert(Utils.nanSafeCompareFloats(Float.PositiveInfinity, Float.NaN) === -1)
    assert(Utils.nanSafeCompareFloats(Float.NegativeInfinity, Float.NaN) === -1)
  }

  test("isDynamicAllocationEnabled") {//动态分配是否可用
    val conf = new SparkConf()
    assert(Utils.isDynamicAllocationEnabled(conf) === false)
    assert(Utils.isDynamicAllocationEnabled(
      conf.set("spark.dynamicAllocation.enabled", "false")) === false)
    assert(Utils.isDynamicAllocationEnabled(
      conf.set("spark.dynamicAllocation.enabled", "true")) === true)
    assert(Utils.isDynamicAllocationEnabled(
      conf.set("spark.executor.instances", "1")) === false)
    assert(Utils.isDynamicAllocationEnabled(
      conf.set("spark.executor.instances", "0")) === true)
  }

}
