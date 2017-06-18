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

import java.io._
import java.net.URI
import java.util.jar.{JarEntry, JarOutputStream}
import javax.net.ssl.SSLException

import com.google.common.io.{ByteStreams, Files}
import org.apache.commons.lang3.RandomUtils

import org.apache.spark.util.Utils

import SSLSampleConfigs._

class FileServerSuite extends SparkFunSuite with LocalSparkContext {

  @transient var tmpDir: File = _
  @transient var tmpFile: File = _
  @transient var tmpJarUrl: String = _
 //是否启用内部身份验证
  def newConf: SparkConf = new SparkConf(loadDefaults = false).set("spark.authenticate", "false")

  override def beforeEach() {
    super.beforeEach()
    resetSparkContext()
  }

  override def beforeAll() {
    super.beforeAll()

    tmpDir = Utils.createTempDir()
    val testTempDir = new File(tmpDir, "test")
    testTempDir.mkdir()

    val textFile = new File(testTempDir, "FileServerSuite.txt")
    val pw = new PrintWriter(textFile)
    // scalastyle:off println
    pw.println("100")
    // scalastyle:on println
    pw.close()//存储FileServerSuite文件

    val jarFile = new File(testTempDir, "test.jar")
    val jarStream = new FileOutputStream(jarFile)
    //jar文件输出流
    val jar = new JarOutputStream(jarStream, new java.util.jar.Manifest())

    val jarEntry = new JarEntry(textFile.getName)
    jar.putNextEntry(jarEntry)

    val in = new FileInputStream(textFile)
    ByteStreams.copy(in, jar)

    in.close()
    jar.close()
    jarStream.close()

    tmpFile = textFile
    tmpJarUrl = jarFile.toURI.toURL.toString
  }

  override def afterAll() {
    super.afterAll()
    Utils.deleteRecursively(tmpDir)
  }

  test("Distributing files locally") {//分布式本地文件
    sc = new SparkContext("local[4]", "test", newConf)
    sc.addFile(tmpFile.toString)
    val testData = Array((1, 1), (1, 1), (2, 1), (3, 5), (2, 2), (3, 0))
    val result = sc.parallelize(testData).reduceByKey {
      val path = SparkFiles.get("FileServerSuite.txt") //读取文件路径
      val in = new BufferedReader(new FileReader(path))
      val fileVal = in.readLine().toInt //读取数据100
      in.close()
      _ * fileVal + _ * fileVal
    }.collect()
    assert(result.toSet === Set((1, 200), (2, 300), (3, 500)))
  }

  test("Distributing files locally security On") {//分布式本地安全文件
    val sparkConf = new SparkConf(false)
    //是否启用内部身份验证
    sparkConf.set("spark.authenticate", "true")
    //设置组件之间进行身份验证的密钥
    sparkConf.set("spark.authenticate.secret", "good")
    sc = new SparkContext("local[4]", "test", sparkConf)

    sc.addFile(tmpFile.toString)
    assert(sc.env.securityManager.isAuthenticationEnabled() === true)
    val testData = Array((1, 1), (1, 1), (2, 1), (3, 5), (2, 2), (3, 0))
    val result = sc.parallelize(testData).reduceByKey {
      val path = SparkFiles.get("FileServerSuite.txt")
      val in = new BufferedReader(new FileReader(path))
      val fileVal = in.readLine().toInt
      in.close()
      _ * fileVal + _ * fileVal
    }.collect()
    assert(result.toSet === Set((1, 200), (2, 300), (3, 500)))
  }
  //分布式以网址作为输入本地文件
  test("Distributing files locally using URL as input") {
    // addFile("file:///....")
    sc = new SparkContext("local[4]", "test", newConf)
    sc.addFile(new File(tmpFile.toString).toURI.toString)
    val testData = Array((1, 1), (1, 1), (2, 1), (3, 5), (2, 2), (3, 0))
    val result = sc.parallelize(testData).reduceByKey {
      val path = SparkFiles.get("FileServerSuite.txt")
      val in = new BufferedReader(new FileReader(path))
      val fileVal = in.readLine().toInt
      in.close()
      _ * fileVal + _ * fileVal
    }.collect()
    assert(result.toSet === Set((1, 200), (2, 300), (3, 500)))
  }
  //动态添加本地Jar
  test ("Dynamically adding JARS locally") {
    sc = new SparkContext("local[4]", "test", newConf)
    sc.addJar(tmpJarUrl)
    val testData = Array((1, 1))
    sc.parallelize(testData).foreach { x =>
      if (Thread.currentThread.getContextClassLoader.getResource("FileServerSuite.txt") == null) {
        throw new SparkException("jar not added")
      }
    }
  }
  //一个独立的群集上分布式文件
  test("Distributing files on a standalone cluster") {
    //sc = new SparkContext("local-cluster[1,1,1024]", "test", newConf)
    sc = new SparkContext("local[*]", "test", newConf)
    sc.addFile(tmpFile.toString)
    val testData = Array((1, 1), (1, 1), (2, 1), (3, 5), (2, 2), (3, 0))
    val result = sc.parallelize(testData).reduceByKey {
      val path = SparkFiles.get("FileServerSuite.txt")
      val in = new BufferedReader(new FileReader(path))
      val fileVal = in.readLine().toInt
      in.close()
      _ * fileVal + _ * fileVal
    }.collect()
    assert(result.toSet === Set((1, 200), (2, 300), (3, 500)))
  }
  //独立的集群上动态添加jar
  test ("Dynamically adding JARS on a standalone cluster") {
    //sc = new SparkContext("local-cluster[1,1,1024]", "test", newConf)
    sc = new SparkContext("local[*]", "test", newConf)
    sc.addJar(tmpJarUrl)
    val testData = Array((1, 1))
    sc.parallelize(testData).foreach { x =>
      if (Thread.currentThread.getContextClassLoader.getResource("FileServerSuite.txt") == null) {
        throw new SparkException("jar not added")
      }
    }
  }
  //独立的集群中动态添加本地的jar和URL
  test ("Dynamically adding JARS on a standalone cluster using local: URL") {
    //sc = new SparkContext("local-cluster[1,1,1024]", "test", newConf)
    sc = new SparkContext("local[*]", "test", newConf)
    sc.addJar(tmpJarUrl.replace("file", "local"))
    val testData = Array((1, 1))
    sc.parallelize(testData).foreach { x =>
      if (Thread.currentThread.getContextClassLoader.getResource("FileServerSuite.txt") == null) {
        throw new SparkException("jar not added")
      }
    }
  }

  test ("HttpFileServer should work with SSL") {
    val sparkConf = sparkSSLConfig()
    val sm = new SecurityManager(sparkConf)
    val server = new HttpFileServer(sparkConf, sm, 0)
    try {
      server.initialize()

      fileTransferTest(server, sm)
    } finally {
      server.stop()
    }
  }

  test ("HttpFileServer should work with SSL and good credentials") {
    val sparkConf = sparkSSLConfig()
     //是否启用内部身份验证
    sparkConf.set("spark.authenticate", "true")
     //设置组件之间进行身份验证的密钥
    sparkConf.set("spark.authenticate.secret", "good")

    val sm = new SecurityManager(sparkConf)
    val server = new HttpFileServer(sparkConf, sm, 0)
    try {
      server.initialize()

      fileTransferTest(server, sm)
    } finally {
      server.stop()
    }
  }
  //HttpFileServer服务不应与有效的SSL和坏的凭据
  test ("HttpFileServer should not work with valid SSL and bad credentials") {
    val sparkConf = sparkSSLConfig()
     //是否启用内部身份验证
    sparkConf.set("spark.authenticate", "true")
     //设置组件之间进行身份验证的密钥
    sparkConf.set("spark.authenticate.secret", "bad")

    val sm = new SecurityManager(sparkConf)
    val server = new HttpFileServer(sparkConf, sm, 0)
    try {
      server.initialize()

      intercept[IOException] {
        fileTransferTest(server)
      }
    } finally {
      server.stop()
    }
  }

  test ("HttpFileServer should not work with SSL when the server is untrusted") {
    val sparkConf = sparkSSLConfigUntrusted()
    val sm = new SecurityManager(sparkConf)
    val server = new HttpFileServer(sparkConf, sm, 0)
    try {
      server.initialize()

      intercept[SSLException] {
        fileTransferTest(server)
      }
    } finally {
      server.stop()
    }
  }

  def fileTransferTest(server: HttpFileServer, sm: SecurityManager = null): Unit = {
    val randomContent = RandomUtils.nextBytes(100)
    val file = File.createTempFile("FileServerSuite", "sslTests", tmpDir)
    Files.write(randomContent, file)
    server.addFile(file)

    val uri = new URI(server.serverUri + "/files/" + file.getName)

    val connection = if (sm != null && sm.isAuthenticationEnabled()) {
      Utils.constructURIForAuthentication(uri, sm).toURL.openConnection()
    } else {
      uri.toURL.openConnection()
    }

    if (sm != null) {
      Utils.setupSecureURLConnection(connection, sm)
    }

    val buf = ByteStreams.toByteArray(connection.getInputStream)
    assert(buf === randomContent)
  }

}
