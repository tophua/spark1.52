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

import java.io._
import java.lang.management.ManagementFactory
import java.net._
import java.nio.ByteBuffer
import java.util.concurrent._
import java.util.{Locale, Properties, Random, UUID}
import javax.net.ssl.HttpsURLConnection

import scala.collection.JavaConversions._
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.reflect.ClassTag
import scala.util.Try
import scala.util.control.{ControlThrowable, NonFatal}

import com.google.common.io.{ByteStreams, Files}
import com.google.common.net.InetAddresses
import org.apache.commons.lang3.SystemUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.log4j.PropertyConfigurator
import org.eclipse.jetty.util.MultiException
import org.json4s._
import tachyon.TachyonURI
import tachyon.client.{TachyonFS, TachyonFile}

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, SerializerInstance}

/**
  * CallSite represents a place in user code. It can have a short and a long form.
  * CallSite代表用户代码中的一个位置,它可以有一个短而长的形式
  * */
private[spark] case class CallSite(shortForm: String, longForm: String)

private[spark] object CallSite {
  val SHORT_FORM = "callSite.short"
  val LONG_FORM = "callSite.long"
}

/**
 * Various utility methods used by Spark.
 * Spark所用的各种实用方法
 */
private[spark] object Utils extends Logging {
  val random = new Random()

  /**
   * Define a default value for driver memory here since this value is referenced across the code
   * base and nearly all files already use Utils.scala
   * 这里定义驱动程序内存的默认值,因为该值是跨代码引用的基础和几乎所有的文件已经使用Utils.scala
   */
  val DEFAULT_DRIVER_MEM_MB = JavaUtils.DEFAULT_DRIVER_MEM_MB.toInt
  //创建最大目录次数
  private val MAX_DIR_CREATION_ATTEMPTS: Int = 10
  @volatile private var localRootDirs: Array[String] = null


  /** 
   *  Serialize an object using Java serialization
   *  使用java序列化序列化一个对象,返回二进数组
   *  */
  def serialize[T](o: T): Array[Byte] = {
    //字节数组输出流在内存中创建一个字节数组缓冲区，转换成字节数组
    val bos = new ByteArrayOutputStream()
    //对基本数据和对象进行序列化操作支持
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close()
    bos.toByteArray
  }

  /** 
   *  Deserialize an object using Java serialization 
   *  使用java序列化反序列化一个对象
   *  */
  def deserialize[T](bytes: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    //来实现序列化和反序列化的操作
    ois.readObject.asInstanceOf[T]
  }

  /**
    * Deserialize an object using Java serialization and the given ClassLoader
    * 使用Java序列化和给定的ClassLoader对对象进行反序列化
    * */
  def deserialize[T](bytes: Array[Byte], loader: ClassLoader): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis) {
      override def resolveClass(desc: ObjectStreamClass): Class[_] = {
        // scalastyle:off classforname
        Class.forName(desc.getName, false, loader)
        // scalastyle:on classforname
      }
    }
    ois.readObject.asInstanceOf[T]
  }

  /** 
   *  Deserialize a Long value (used for [[org.apache.spark.api.python.PythonPartitioner]]) 
   *  反序列化
   *  */
  def deserializeLongValue(bytes: Array[Byte]) : Long = {
    // Note: we assume that we are given a Long value encoded in network (big-endian) byte order
    //注意：我们假设我们被给予一个以网络（大端）字节顺序编码的长值
    var result = bytes(7) & 0xFFL
    result = result + ((bytes(6) & 0xFFL) << 8)
    result = result + ((bytes(5) & 0xFFL) << 16)
    result = result + ((bytes(4) & 0xFFL) << 24)
    result = result + ((bytes(3) & 0xFFL) << 32)
    result = result + ((bytes(2) & 0xFFL) << 40)
    result = result + ((bytes(1) & 0xFFL) << 48)
    result + ((bytes(0) & 0xFFL) << 56)
  }

  /**
    * Serialize via nested stream using specific serializer
    * 使用特定的序列化程序通过嵌套流序列化
    * */
  def serializeViaNestedStream(os: OutputStream, ser: SerializerInstance)(
      f: SerializationStream => Unit): Unit = {
    val osWrapper = ser.serializeStream(new OutputStream {
      override def write(b: Int): Unit = os.write(b)
      override def write(b: Array[Byte], off: Int, len: Int): Unit = os.write(b, off, len)
    })
    try {
      f(osWrapper)
    } finally {
      osWrapper.close()
    }
  }

  /**
    * Deserialize via nested stream using specific serializer
    * 使用特定的序列化器通过嵌套流反序列化
    * */
  def deserializeViaNestedStream(is: InputStream, ser: SerializerInstance)(
      f: DeserializationStream => Unit): Unit = {
    val isWrapper = ser.deserializeStream(new InputStream {
      override def read(): Int = is.read()
      override def read(b: Array[Byte], off: Int, len: Int): Int = is.read(b, off, len)
    })
    try {
      f(isWrapper)
    } finally {
      isWrapper.close()
    }
  }

  /**
   * 获取加载当前class的ClassLoader
   * Get the ClassLoader which loaded Spark.
   */
  def getSparkClassLoader: ClassLoader = getClass.getClassLoader

  /**
   * Get the Context ClassLoader on this thread or, if not present, the ClassLoader that
   * loaded Spark.
   * 用于获取线程上下文的classloader,没有设置时获取加载Spark的classLoader
   * This should be used whenever passing a ClassLoader to Class.ForName or finding the currently
   * active loader when setting up ClassLoader delegation chains.
    * 当将ClassLoader传递给Class.ForName或在设置ClassLoader委派链时查找当前活动的加载程序时,应使用这一点,
   */
  def getContextOrSparkClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)

  /** 
   *  Determines whether the provided class is loadable in the current thread. 
   *  是否类加载器在当前线程中
   *  */
  def classIsLoadable(clazz: String): Boolean = {
    // scalastyle:off classforname
    //Try: 解决函数可能会抛出异常问题
    //Try来包裹它，得到Try的子类Success或者Failure，如果计算成功，返回Success的实例，如果抛出异常，返回Failure并携带相关信息
    Try { Class.forName(clazz, false, getContextOrSparkClassLoader) }.isSuccess
    // scalastyle:on classforname
  }

  // scalastyle:off classforname
  /**
    * Preferred alternative to Class.forName(className)
    * Class.forName(className)的首选替代方法
    *  */
  def classForName(className: String): Class[_] = {
    Class.forName(className, true, getContextOrSparkClassLoader)
    // scalastyle:on classforname
  }

  /**
   * Primitive often used when writing [[java.nio.ByteBuffer]] to [[java.io.DataOutput]]
   * 使用原始nio.ByteBuffer写到io.DataOutput操作
   */
  def writeByteBuffer(bb: ByteBuffer, out: ObjectOutput): Unit = {
    //来检查是否支持访问数组
    if (bb.hasArray) {
      //arrayOffset 返回此缓冲区中的第一个元素在缓冲区的底层实现数组中的偏移量
      //position 即缓冲区开始读或者写数据的位置
      //remaining 返回剩余的可用长度,此长度为实际读取的数据长度
      //array 返回支持此缓冲区的字节数组
      out.write(bb.array(), bb.arrayOffset() + bb.position(), bb.remaining())
    } else {
      //remaining 返回剩余的可用长度,此长度为实际读取的数据长度
      val bbval = new Array[Byte](bb.remaining())
      bb.get(bbval)
      out.write(bbval)
    }
  }

  /**
   * JDK equivalent of `chmod 700 file`.
    * JDK相当于`chmod 700 file`
   *
   * @param file the file whose permissions will be modified 该文件的权限将被修改
   * @return true if the permissions were successfully changed, false otherwise.
    *         如果权限成功更改,则为true,否则为false,
   */
  def chmod700(file: File): Boolean = {
    file.setReadable(false, false) &&
    file.setReadable(true, true) &&
    file.setWritable(false, false) &&
    file.setWritable(true, true) &&
    file.setExecutable(false, false) &&
    file.setExecutable(true, true)
  }

  /**
   * Create a directory inside the given parent directory. The directory is guaranteed to be
   * newly created, and is not marked for automatic deletion.
   * 用Spark+UUID的方式创建临时文件目录,如果创建失败会多次重试,最多重试10次.
   */
  def createDirectory(root: String, namePrefix: String = "spark"): File = {
    var attempts = 0
    //最大目录尝试次数
    val maxAttempts = MAX_DIR_CREATION_ATTEMPTS
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory (under " + root + ") after " +
          maxAttempts + " attempts!")
      }
      try {
        dir = new File(root, namePrefix + "-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {//生成目录不成功
          dir = null
        }
      } catch { case e: SecurityException => dir = null; }
    }
    //返回不带.号的绝对路径
    dir.getCanonicalFile
  }

  /**
   * 在Spark一级目录下创建临时目录,并将目录注册到shutdownDeletePaths = new scala.collection.mutable.HashSet[String]()中   * 
   * Create a temporary directory inside the given parent directory. The directory will be
   * automatically deleted when the VM shuts down.
   */
  def createTempDir(
      root: String = System.getProperty("java.io.tmpdir"),//根据目录目录默认tmpdir
      namePrefix: String = "spark"): File = {
    val dir = createDirectory(root, namePrefix)
    ShutdownHookManager.registerShutdownDeleteDir(dir)
    dir
  }

  /** 
   *  Copy all data from an InputStream to an OutputStream. NIO way of file stream to file stream
    * copying is disabled by default unless explicitly set transferToEnabled as true,
    * 将InputStream中的所有数据复制到OutputStream。 NIO文件流到文件流默认情况下禁用复制,除非将transferToEnabled设置为true
    * the parameter transferToEnabled should be configured by spark.file.transferTo = [true|false].
    * 复制数据从InputStream输入流 到OutputStream输出流,
    */
  def copyStream(in: InputStream,
                 out: OutputStream,
                 closeStreams: Boolean = false,
                 transferToEnabled: Boolean = false): Long =
  {
    var count = 0L
    tryWithSafeFinally {
      if (in.isInstanceOf[FileInputStream] && out.isInstanceOf[FileOutputStream]
        && transferToEnabled) {
        // When both streams are File stream, use transferTo to improve copy performance.
        //当两个流都是文件流时,使用transferTo来提高复制性能
        val inChannel = in.asInstanceOf[FileInputStream].getChannel()
        val outChannel = out.asInstanceOf[FileOutputStream].getChannel()
        val initialPos = outChannel.position()
        val size = inChannel.size()

        // In case transferTo method transferred less data than we have required.
        //如果transferTo方法传输的数据少于我们所需要的数据
        while (count < size) {
          count += inChannel.transferTo(count, size - count, outChannel)
        }

        // Check the position after transferTo loop to see if it is in the right position and
        // give user information if not.
        //检查转移后的位置循环查看是否在正确的位置,如果没有,给予用户信息
        // Position will not be increased to the expected length after calling transferTo in
        // kernel version 2.6.32, this issue can be seen in
        // https://bugs.openjdk.java.net/browse/JDK-7052359
        // This will lead to stream corruption issue when using sort-based shuffle (SPARK-3948).
        val finalPos = outChannel.position()
        assert(finalPos == initialPos + size,
          s"""
             |Current position $finalPos do not equal to expected position ${initialPos + size}
             |after transferTo, please check your kernel version to see if it is 2.6.32,
             |this is a kernel bug which will lead to unexpected behavior when using transferTo.
             |You can set spark.file.transferTo = false to disable this NIO feature.
           """.stripMargin)
      } else {
        val buf = new Array[Byte](8192)
        var n = 0
        while (n != -1) {
          n = in.read(buf)
          if (n != -1) {
            out.write(buf, 0, n)
            count += n
          }
        }
      }
      count
    } {
      if (closeStreams) {
        try {
          in.close()
        } finally {
          out.close()
        }
      }
    }
  }

  /**
   * Construct a URI container information used for authentication.
   * 构建一个用于身份验证的URI的集装箱信息,
   * This also sets the default authenticator to properly negotiation the
   * user/password based on the URI.
    *
   * 这还设置默认验证器,以根据URI正确协商用户/密码
    *
   * Note this relies on the Authenticator.setDefault being set properly to decode
   * the user name and password. This is currently set in the SecurityManager.
    * 请注意,这取决于Authenticator.setDefault正确设置以解码用户名和密码,这是当前在SecurityManager中设置的
   */
  def constructURIForAuthentication(uri: URI, securityMgr: SecurityManager): URI = {
    val userCred = securityMgr.getSecretKey()
    if (userCred == null) throw new Exception("Secret key is null with authentication on")
    val userInfo = securityMgr.getHttpUser()  + ":" + userCred
    new URI(uri.getScheme(), userInfo, uri.getHost(), uri.getPort(), uri.getPath(),
      uri.getQuery(), uri.getFragment())
  }

  /**
    *将文件或目录下载到目标目录,支持基于URL参数以各种方式获取文件,包括HTTP,Hadoop兼容文件系统和标准文件系统上的文件,
    * 只能从Hadoop兼容的文件系统中获取获取目录。
   * Download a file or directory to target directory. Supports fetching the file in a variety of
   * ways, including HTTP, Hadoop-compatible filesystems, and files on a standard filesystem, based
   * on the URL parameter. Fetching directories is only supported from Hadoop-compatible
   * filesystems.
   *
   * If `useCache` is true, first attempts to fetch the file to a local cache that's shared
   * across executors running the same application. `useCache` is used mainly for
   * the executors, and not in local mode.
    * 如果useCache为true,则首先尝试将文件提取到在运行相同应用程序的执行程序之间共享的本地缓存,
    * useCache主要用于执行程序,而不是在本地模式下使用。
   *
   * Throws SparkException if the target file already exists and has different contents than
   * the requested file.
    * 如果目标文件已经存在并且具有与请求的文件不同的内容,则抛出SparkException。
   */
  def fetchFile(
      url: String,
      targetDir: File,
      conf: SparkConf,
      securityMgr: SecurityManager,
      hadoopConf: Configuration,
      timestamp: Long,
      useCache: Boolean) {
    val fileName = url.split("/").last
    val targetFile = new File(targetDir, fileName)
    val fetchCacheEnabled = conf.getBoolean("spark.files.useFetchCache", defaultValue = true)
    if (useCache && fetchCacheEnabled) {
      val cachedFileName = s"${url.hashCode}${timestamp}_cache"
      val lockFileName = s"${url.hashCode}${timestamp}_lock"
      val localDir = new File(getLocalDir(conf))
      val lockFile = new File(localDir, lockFileName)
      //RandomAccessFile 此类的实例支持对随机访问文件的读取和写入
      val lockFileChannel = new RandomAccessFile(lockFile, "rw").getChannel()
      // Only one executor entry.
      // The FileLock is only used to control synchronization for executors download file,
      // it's always safe regardless of lock type (mandatory or advisory).
      //只有一个执行者条目,FileLock仅用于控制执行程序下载文件的同步,无论锁定类型（强制或咨询）如何,它始终是安全的。
      val lock = lockFileChannel.lock()
      val cachedFile = new File(localDir, cachedFileName)
      try {
        if (!cachedFile.exists()) {
          doFetchFile(url, localDir, cachedFileName, conf, securityMgr, hadoopConf)
        }
      } finally {
        lock.release()
        lockFileChannel.close()
      }
      copyFile(
        url,
        cachedFile,
        targetFile,
	    //通过 SparkContext.addFile() 添加的文件在目标中已经存在并且内容不匹配时,是否覆盖目标文件
        conf.getBoolean("spark.files.overwrite", false)
      )
    } else {
      doFetchFile(url, targetDir, fileName, conf, securityMgr, hadoopConf)
    }

    // Decompress the file if it's a .tar or .tar.gz
    //解压文件,如果是.tar或.tar.gz
    if (fileName.endsWith(".tar.gz") || fileName.endsWith(".tgz")) {
      logInfo("Untarring " + fileName)
      //解压.tar.gz文件
      executeAndGetOutput(Seq("tar", "-xzf", fileName), targetDir)
    } else if (fileName.endsWith(".tar")) {
      logInfo("Untarring " + fileName)
       //解压.tar文件
      executeAndGetOutput(Seq("tar", "-xf", fileName), targetDir)
    }
    // Make the file executable - That's necessary for scripts
    //使文件可执行 - 脚本是必需的
    FileUtil.chmod(targetFile.getAbsolutePath, "a+x")

    // Windows does not grant read permission by default to non-admin users
    // Add read permission to owner explicitly
    //默认情况下，Windows不会向非管理员用户授予读取权限
    //显式添加读取权限给所有者
    if (isWindows) {
      FileUtil.chmod(targetFile.getAbsolutePath, "u+r")
    }
  }

  /**
   * Download in to tempFile, then move it to destFile.
   * 下载到 tempfile ,然后将它移到destfile
   * If destFile already exists:
    * 如果destFile已经存在：
   *   - no-op if its contents equal those of sourceFile,
    *     如果其内容与sourceFile的内容相同，那么no-op，
   *   - throw an exception if fileOverwrite is false,
    *   如果fileOverwrite为false，则抛出异常，
   *   - attempt to overwrite it otherwise.否则尝试覆盖它。
   *
   * @param url URL that sourceFile originated from, for logging purposes.s
    *            ourceFile源自的URL,用于记录目的
   * @param in InputStream to download. InputStream下载
   * @param destFile File path to move tempFile to.将tempFile移动到的文件路径
   * @param fileOverwrite Whether to delete/overwrite an existing destFile that does not match
   *                      sourceFile
    *                      是否删除/覆盖与sourceFile不匹配的现有destFile
   */
  private def downloadFile(
      url: String,
      in: InputStream,
      destFile: File,
      fileOverwrite: Boolean): Unit = {
    val tempFile = File.createTempFile("fetchFileTemp", null,
      new File(destFile.getParentFile.getAbsolutePath))
    logInfo(s"Fetching $url to $tempFile")

    try {
      val out = new FileOutputStream(tempFile)
      Utils.copyStream(in, out, closeStreams = true)
      copyFile(url, tempFile, destFile, fileOverwrite, removeSourceFile = true)
    } finally {
      // Catch-all for the couple of cases where for some reason we didn't move `tempFile` to
      // `destFile`.
      //抓住所有的情况,由于某些原因,我们没有将`tempFile'移动到`destFile'。
      if (tempFile.exists()) {
        tempFile.delete()
      }
    }
  }

  /**
   * Copy `sourceFile` to `destFile`.
   * 复制源文件到目录文件
   * If `destFile` already exists:
    * 如果`destFile`已经存在：
   *   - no-op if its contents equal those of `sourceFile`,如果其内容等于`sourceFile`，
   *   - throw an exception if `fileOverwrite` is false,如果fileOverwrite为false，则抛出异常，
   *   - attempt to overwrite it otherwise.否则尝试覆盖它
   *
   * @param url URL that `sourceFile` originated from, for logging purposes.
    *            “sourceFile”源自的URL，用于记录目的
   * @param sourceFile File path to copy/move from.复制/移动的文件路径
   * @param destFile File path to copy/move to.复制/移动到的文件路径
   * @param fileOverwrite Whether to delete/overwrite an existing `destFile` that does not match
   *                      `sourceFile`是否删除/覆盖与`sourceFile`不匹配的现有`destFile`
   * @param removeSourceFile Whether to remove `sourceFile` after / as part of moving/copying it to
   *                         `destFile`.
    *                         是否删除`sourceFile`之后/作为移动/复制到`destFile`的一部分
   */
  private def copyFile(
      url: String,
      sourceFile: File,
      destFile: File,
      fileOverwrite: Boolean,//文件覆盖
      //removeSourceFile 删除源文件
      removeSourceFile: Boolean = false): Unit = {
    if (destFile.exists) {
      if (!filesEqualRecursive(sourceFile, destFile)) {
        if (fileOverwrite) {
          logInfo(
            s"File $destFile exists and does not match contents of $url, replacing it with $url"
          )
          if (!destFile.delete()) {
            throw new SparkException(
              "Failed to delete %s while attempting to overwrite it with %s".format(
                destFile.getAbsolutePath,
                sourceFile.getAbsolutePath
              )
            )
          }
        } else {
          throw new SparkException(
            s"File $destFile exists and does not match contents of $url")
        }
      } else {
        // Do nothing if the file contents are the same, i.e. this file has been copied
        // previously.
        //如果文件内容相同,则不执行任何操作,即此文件以前已被复制。
        logInfo(
          "%s has been previously copied to %s".format(
            sourceFile.getAbsolutePath,
            destFile.getAbsolutePath
          )
        )
        return
      }
    }

    // The file does not exist in the target directory. Copy or move it there.
    //文件不存在于目标目录中,复制或移动它
    if (removeSourceFile) {
      Files.move(sourceFile, destFile)
    } else {
      logInfo(s"Copying ${sourceFile.getAbsolutePath} to ${destFile.getAbsolutePath}")
      copyRecursive(sourceFile, destFile)
    }
  }
  //文件相等递归
  private def filesEqualRecursive(file1: File, file2: File): Boolean = {
    if (file1.isDirectory && file2.isDirectory) {
      val subfiles1 = file1.listFiles()
      val subfiles2 = file2.listFiles()
      if (subfiles1.size != subfiles2.size) {
        return false
      }
      subfiles1.sortBy(_.getName).zip(subfiles2.sortBy(_.getName)).forall {
        case (f1, f2) => filesEqualRecursive(f1, f2)
      }
    } else if (file1.isFile && file2.isFile) {
      Files.equal(file1, file2)
    } else {
      false
    }
  }
  //递归复制
  private def copyRecursive(source: File, dest: File): Unit = {
    if (source.isDirectory) {
      if (!dest.mkdir()) {
        throw new IOException(s"Failed to create directory ${dest.getPath}")
      }
      //子文件
      val subfiles = source.listFiles()
      //dest父抽象路径名,child 子路径名字符串
      //File 通过给定的父抽象路径名和子路径名字符串创建一个新的File实例
      subfiles.foreach(f => copyRecursive(f, new File(dest, f.getName)))
    } else {
      Files.copy(source, dest)
    }
  }

  /**
   * 将文件或目录下载到目标目录,支持基于URL参数以各种方式获取文件,
    * 包括HTTP,Hadoop兼容文件系统和标准文件系统上的文件。只能从Hadoop兼容的文件系统中获取获取目录。
   * Download a file or directory to target directory. Supports fetching the file in a variety of
   * ways, including HTTP, Hadoop-compatible filesystems, and files on a standard filesystem, based
   * on the URL parameter. Fetching directories is only supported from Hadoop-compatible
   * filesystems.
   *
   * Throws SparkException if the target file already exists and has different contents than
   * the requested file.
    * 如果目标文件已经存在并且具有与请求的文件不同的内容,则抛出SparkException。
   */
  private def doFetchFile(
      url: String,
      targetDir: File,
      filename: String,
      conf: SparkConf,
      securityMgr: SecurityManager,
      hadoopConf: Configuration) {
    val targetFile = new File(targetDir, filename)
    val uri = new URI(url)
    //通过 SparkContext.addFile() 添加的文件在目标中已经存在并且内容不匹配时,是否覆盖目标文件
    val fileOverwrite = conf.getBoolean("spark.files.overwrite", defaultValue = false)
    //getScheme 返回当前链接使用的协议,getOrElse输出默认值file
    Option(uri.getScheme).getOrElse("file") match {
      case "http" | "https" | "ftp" =>
        var uc: URLConnection = null
        if (securityMgr.isAuthenticationEnabled()) {
          logDebug("fetchFile with security enabled")
          val newuri = constructURIForAuthentication(uri, securityMgr)
          uc = newuri.toURL().openConnection()
          //设置该URLConnection的allowUserInteraction请求头字段的值
          uc.setAllowUserInteraction(false)
        } else {
          logDebug("fetchFile not using security")
          uc = new URL(url).openConnection()
        }
        Utils.setupSecureURLConnection(uc, securityMgr)
	      //在获取由driver通过SparkContext.addFile()添加的文件时,是否使用通信时间超时
        val timeoutMs =
          conf.getTimeAsSeconds("spark.files.fetchTimeout", "60s").toInt * 1000
        uc.setConnectTimeout(timeoutMs)
        uc.setReadTimeout(timeoutMs)
        uc.connect()
        val in = uc.getInputStream()
        downloadFile(url, in, targetFile, fileOverwrite)
      case "file" =>
        // In the case of a local file, copy the local file to the target directory.
        // Note the difference between uri vs url.
        //在本地文件的情况下.将本地文件复制到目标目录.
        //注意uri和url之间的区别
        val sourceFile = if (uri.isAbsolute) new File(uri) else new File(url)
        copyFile(url, sourceFile, targetFile, fileOverwrite)
      case _ =>
        val fs = getHadoopFileSystem(uri, hadoopConf)
        val path = new Path(uri)
        fetchHcfsFile(path, targetDir, fs, conf, hadoopConf, fileOverwrite,
                      filename = Some(filename))
    }
  }

  /**
   * Fetch a file or directory from a Hadoop-compatible filesystem.
   * 从Hadoop兼容的文件系统中获取文件或目录。
   * Visible for testing
    * 可见测试
   */
  private[spark] def fetchHcfsFile(
      path: Path,
      targetDir: File,
      fs: FileSystem,
      conf: SparkConf,
      hadoopConf: Configuration,
      fileOverwrite: Boolean,
      filename: Option[String] = None): Unit = {
    if (!targetDir.exists() && !targetDir.mkdir()) {
      throw new IOException(s"Failed to create directory ${targetDir.getPath}")
    }
    val dest = new File(targetDir, filename.getOrElse(path.getName))
    if (fs.isFile(path)) {
      val in = fs.open(path)
      try {
        downloadFile(path.toString, in, dest, fileOverwrite)
      } finally {
        in.close()
      }
    } else {
      fs.listStatus(path).foreach { fileStatus =>
        fetchHcfsFile(fileStatus.getPath(), dest, fs, conf, hadoopConf, fileOverwrite)
      }
    }
  }

  /**
   * 获取临时目录的路径, Spark的本地目录可以通过多个设置进行配置,这些设置具有以下优先级：
   * Get the path of a temporary directory.  Spark's local directories can be configured through
   * multiple settings, which are used with the following precedence:
   *
   *   - If called from inside of a YARN container, this will return a directory chosen by YARN.
    *     如果从YARN容器内部调用,将返回YARN选择的目录。
   *   - If the SPARK_LOCAL_DIRS environment variable is set, this will return a directory from it.
    *     如果设置了SPARK_LOCAL_DIRS环境变量,这将返回一个目录。
   *   - Otherwise, if the spark.local.dir is set, this will return a directory from it.
    *   否则,如果设置了spark.local.dir,它将从中返回一个目录。
   *   - Otherwise, this will return java.io.tmpdir.否则,这将返回java.io.tmpdir。
   *
   * Some of these configuration options might be lists of multiple paths, but this method will
   * always return a single directory.
    * 这些配置选项中的一些可能是多个路径的列表,但此方法将始终返回单个目录。
   */
  def getLocalDir(conf: SparkConf): String = {
    getOrCreateLocalRootDirs(conf)(0)
  }

  private[spark] def isRunningInYarnContainer(conf: SparkConf): Boolean = {
    // These environment variables are set by YARN.这些环境变量由YARN设置
    // For Hadoop 0.23.X, we check for YARN_LOCAL_DIRS (we use this below in getYarnLocalDirs())
    // For Hadoop 2.X, we check for CONTAINER_ID.
    //对于Hadoop 0.23.X，我们检查YARN_LOCAL_DIRS(我们在getYarnLocalDirs()中使用这个)
    //对于Hadoop 2.X，我们检查CONTAINER_ID。
    conf.getenv("CONTAINER_ID") != null || conf.getenv("YARN_LOCAL_DIRS") != null
  }

  /**
   * 根据spark.local.dir的配置,作为本地文件的根目录,在创建一,二级目录之前要确保目录是存在的,然后调用
   * getOrCreateLocalRootDirsImpl创建一级目录
    * 获取或创建spark.local.dir或SPARK_LOCAL_DIRS中列出的目录,并仅返回已存在/可以创建的目录。
   * Gets or creates the directories listed in spark.local.dir or SPARK_LOCAL_DIRS,
   * and returns only the directories that exist / could be created.
   *
   * If no directories could be created, this will return an empty list.
   * 如果没有创建任何目录，这将返回一个空列表。
   * This method will cache the local directories for the application when it's first invoked.
   * So calling it multiple times with a different configuration will always return the same
   * set of directories.
    * 此方法将在应用程序首次调用时缓存本地目录,所以使用不同的配置多次调用它将始终返回相同的目录集。
   */
  private[spark] def getOrCreateLocalRootDirs(conf: SparkConf): Array[String] = {
    if (localRootDirs == null) {
      this.synchronized {
        if (localRootDirs == null) {
          localRootDirs = getOrCreateLocalRootDirsImpl(conf)
        }
      }
    }
    localRootDirs
  }

  /**
   * 返回Spark可以写入文件的配置的本地目录,该方法本身并不创建任何目录,只能根据部署模式封装查找本地目录的逻辑。
   * Return the configured local directories where Spark can write files. This
   * method does not create any directories on its own, it only encapsulates the
   * logic of locating the local directories according to deployment mode.
   */
  def getConfiguredLocalDirs(conf: SparkConf): Array[String] = {
    if (isRunningInYarnContainer(conf)) {
      // If we are in yarn mode, systems can have different disk layouts so we must set it
      // to what Yarn on this system said was available. Note this assumes that Yarn has
      // created the directories already, and that they are secured so that only the
      // user has access to them.
      //如果我们在yarn模式下，系统可以有不同的磁盘布局,所以我们必须将其设置为该系统上可用的yarn。
      // 请注意，这假设yarn已经创建了目录，并且它们是安全的，以便只有用户可以访问它们。
      getYarnLocalDirs(conf).split(",")
    } else if (conf.getenv("SPARK_EXECUTOR_DIRS") != null) {
      conf.getenv("SPARK_EXECUTOR_DIRS").split(File.pathSeparator)
    } else {
      // In non-Yarn mode (or for the driver in yarn-client mode), we cannot trust the user
      // configuration to point to a secure directory. So create a subdirectory with restricted
      // permissions under each listed directory.
      //在非yarn模式(或yarn客户端模式下的驱动程序)中,我们无法相信用户配置指向安全目录。
      // 因此,在每个列出的目录下创建一个具有受限权限的子目录
      Option(conf.getenv("SPARK_LOCAL_DIRS"))
        .getOrElse(conf.get("spark.local.dir", System.getProperty("java.io.tmpdir")))
        .split(",")
    }
  }

  private def getOrCreateLocalRootDirsImpl(conf: SparkConf): Array[String] = {
    getConfiguredLocalDirs(conf).flatMap { root =>
      try {
        val rootDir = new File(root)
        if (rootDir.exists || rootDir.mkdirs()) {
          val dir = createTempDir(root)
          chmod700(dir)
          Some(dir.getAbsolutePath)
        } else {
          logError(s"Failed to create dir in $root. Ignoring this directory.")
          None
        }
      } catch {
        case e: IOException =>
          logError(s"Failed to create local root dir in $root. Ignoring this directory.")
          None
      }
    }.toArray
  }

  /** Get the Yarn approved local directories.
    * 获得Yarn批准的本地目录。
    * */
  private def getYarnLocalDirs(conf: SparkConf): String = {
    // Hadoop 0.23 and 2.x have different Environment variable names for the
    // local dirs, so lets check both. We assume one of the 2 is set.
    // LOCAL_DIRS => 2.X, YARN_LOCAL_DIRS => 0.23.X
    //Hadoop 0.23和2.x具有不同的环境变量名称为本地dirs,所以让我们检查两者,
    // 我们假设2中的一个设置,LOCAL_DIRS => 2.X,YARN_LOCAL_DIRS => 0.23.X
    val localDirs = Option(conf.getenv("YARN_LOCAL_DIRS"))
      .getOrElse(Option(conf.getenv("LOCAL_DIRS"))
      .getOrElse(""))

    if (localDirs.isEmpty) {
      throw new Exception("Yarn Local dirs can't be empty")
    }
    localDirs
  }

  /**
    * Used by unit tests. Do not call from other places.
    * 单元测试使用,不要从别的地方调用
    *  */
  private[spark] def clearLocalRootDirs(): Unit = {
    localRootDirs = null
  }

  /**
   * Shuffle the elements of a collection into a random order, returning the
   * result in a new collection. Unlike scala.util.Random.shuffle, this method
   * uses a local random number generator, avoiding inter-thread contention.
    * 将集合的元素随机排列为随机顺序,将结果返回到新集合中, 与scala.util.Random.shuffle不同,
    * 该方法使用本地随机数生成器,避免了线程间争用。
   */
  def randomize[T: ClassTag](seq: TraversableOnce[T]): Seq[T] = {
    randomizeInPlace(seq.toArray)
  }

  /**
   * Shuffle the elements of an array into a random order, modifying the
   * original array. Returns the original array.
    *将数组的元素随机排列为随机顺序,修改原始数组,返回原始数组。
   */
  def randomizeInPlace[T](arr: Array[T], rand: Random = new Random): Array[T] = {
    for (i <- (arr.length - 1) to 1 by -1) {
      val j = rand.nextInt(i)
      val tmp = arr(j)
      arr(j) = arr(i)
      arr(i) = tmp
    }
    arr
  }

  /**
   * Get the local host's IP address in dotted-quad format (e.g. 1.2.3.4).
    * 以虚拟四格格式获取本地主机的IP地址
   * Note, this is typically not used from within core spark.
    * 注意,这通常不在核心Spark中使用
    *
   */
  private lazy val localIpAddress: InetAddress = findLocalInetAddress()
/**
 * 查找本地址
 */
  private def findLocalInetAddress(): InetAddress = {
    val defaultIpOverride = System.getenv("SPARK_LOCAL_IP")
    if (defaultIpOverride != null) {
      InetAddress.getByName(defaultIpOverride)
    } else {
      val address = InetAddress.getLocalHost
      if (address.isLoopbackAddress) {
        // Address resolves to something like 127.0.1.1, which happens on Debian; try to find
        // a better address using the local network interfaces
        //地址解析为127.0.1.1,发生在Debian上,尝试使用本地网络接口找到更好的地址
        // getNetworkInterfaces returns ifs in reverse order compared to ifconfig output order
        // on unix-like system. On windows, it returns in index order.
        // It's more proper to pick ip address following system output order.
        //getNetworkInterfaces以相反的顺序返回ifs,与ifix类似系统上的ifconfig输出顺序相比。
        // 在Windows上,它按索引顺序返回。 按照系统输出顺序选择IP地址更合适。
        val activeNetworkIFs = NetworkInterface.getNetworkInterfaces.toList
        val reOrderedNetworkIFs = if (isWindows) activeNetworkIFs else activeNetworkIFs.reverse

        for (ni <- reOrderedNetworkIFs) {
          val addresses = ni.getInetAddresses.toList
            .filterNot(addr => addr.isLinkLocalAddress || addr.isLoopbackAddress)
          if (addresses.nonEmpty) {
            val addr = addresses.find(_.isInstanceOf[Inet4Address]).getOrElse(addresses.head)
            // because of Inet6Address.toHostName may add interface at the end if it knows about it
            val strippedAddress = InetAddress.getByAddress(addr.getAddress)
            // We've found an address that looks reasonable!
            logWarning("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
              " a loopback address: " + address.getHostAddress + "; using " +
              strippedAddress.getHostAddress + " instead (on interface " + ni.getName + ")")
            logWarning("Set SPARK_LOCAL_IP if you need to bind to another address")
            return strippedAddress
          }
        }
        logWarning("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
          " a loopback address: " + address.getHostAddress + ", but we couldn't find any" +
          " external IP address!")
        logWarning("Set SPARK_LOCAL_IP if you need to bind to another address")
      }
      address
    }
  }
  //System.getenv()和System.getProperties()的区别
  //System.getenv() 返回系统环境变量值 设置系统环境变量：当前登录用户主目录下的".bashrc"文件中可以设置系统环境变量
  //System.getProperties() 返回Java进程变量值 通过命令行参数的"-D"选项
  private var customHostname: Option[String] = sys.env.get("SPARK_LOCAL_HOSTNAME")

  /**
   * Allow setting a custom host name because when we run on Mesos we need to use the same
   * hostname it reports to the master.
    *允许设置自定义主机名称,因为当我们在Mesos上运行时,我们需要使用与主机报告相同的主机名。
   */
  def setCustomHostname(hostname: String) {
    // DEBUG code
    Utils.checkHost(hostname)
    customHostname = Some(hostname)
  }

  /**
   * Get the local machine's hostname.
   * 获得本地机器名称
   */
  def localHostName(): String = {
    customHostname.getOrElse(localIpAddress.getHostAddress)
  }

  /**
   * Get the local machine's URI.
    * 获取本地机器的URI
   */
  def localHostNameForURI(): String = {
    customHostname.getOrElse(InetAddresses.toUriString(localIpAddress))
  }

  def checkHost(host: String, message: String = "") {
    assert(host.indexOf(':') == -1, message)
  }

  def checkHostPort(hostPort: String, message: String = "") {
    assert(hostPort.indexOf(':') != -1, message)
  }

  // Typically, this will be of order of number of nodes in cluster
  // If not, we should change it to LRUCache or something.
  //通常,这将是群集中节点数量的顺序
  //如果没有,我们应该将其更改为LRUCache或某些东西
  private val hostPortParseResults = new ConcurrentHashMap[String, (String, Int)]()

  def parseHostPort(hostPort: String): (String, Int) = {
    // Check cache first.先检查缓存
    val cached = hostPortParseResults.get(hostPort)
    if (cached != null) {
      return cached
    }

    val indx: Int = hostPort.lastIndexOf(':')
    // This is potentially broken - when dealing with ipv6 addresses for example, sigh ...
    // but then hadoop does not support ipv6 right now.
    // For now, we assume that if port exists, then it is valid - not check if it is an int > 0
    if (-1 == indx) {
      val retval = (hostPort, 0)
      hostPortParseResults.put(hostPort, retval)
      return retval
    }

    val retval = (hostPort.substring(0, indx).trim(), hostPort.substring(indx + 1).trim().toInt)
    hostPortParseResults.putIfAbsent(hostPort, retval)
    hostPortParseResults.get(hostPort)
  }

  /**
   * Return the string to tell how long has passed in milliseconds.
    * 返回字符串,告诉过了多久时间以毫秒为单位
   */
  def getUsedTimeMs(startTimeMs: Long): String = {
    " " + (System.currentTimeMillis - startTimeMs) + " ms"
  }

  private def listFilesSafely(file: File): Seq[File] = {
    if (file.exists()) {
      val files = file.listFiles()
      if (files == null) {
        throw new IOException("Failed to list files for dir: " + file)
      }
      files
    } else {
      List()
    }
  }

  /**
   * 用于删除文件或删除目录及子目录,子文件,并且从
   * Delete a file or directory and its contents recursively(递归).
   * Don't follow directories if they are symlinks.
   * Throws an exception if deletion is unsuccessful.
    * 如果他们不按照目录的符号链接
    *如果删除失败，则引发异常
   */
  def deleteRecursively(file: File) {
    if (file != null) {
      try {
        if (file.isDirectory && !isSymlink(file)) {
          var savedIOException: IOException = null
          for (child <- listFilesSafely(file)) {
            try {
              deleteRecursively(child)
            } catch {
              // In case of multiple exceptions, only last one will be thrown
              // 如果有多个异常,则只抛出最后一个异常。
              case ioe: IOException => savedIOException = ioe
            }
          }
          if (savedIOException != null) {
            throw savedIOException
          }
          ShutdownHookManager.removeShutdownDeleteDir(file)
        }
      } finally {
        if (!file.delete()) {
          // Delete can also fail if the file simply did not exist
          //如果文件不存在,删除也可能失败。
          if (file.exists()) {
            throw new IOException("Failed to delete: " + file.getAbsolutePath)
          }
        }
      }
    }
  }

  /**
   * Delete a file or directory and its contents recursively.
   * 递归删除一个文件或目录和它的内容。
   */
  def deleteRecursively(dir: TachyonFile, client: TachyonFS) {
    if (!client.delete(new TachyonURI(dir.getPath()), true)) {
      throw new IOException("Failed to delete the tachyon dir: " + dir)
    }
  }

  /**
   * Check to see if file is a symbolic link.
   * 检查文件是否一个符号链接
   */
  def isSymlink(file: File): Boolean = {
    if (file == null) throw new NullPointerException("File must not be null")
    if (isWindows) return false
    val fileInCanonicalDir = if (file.getParent() == null) {
      file
    } else {
      new File(file.getParentFile().getCanonicalFile(), file.getName())
    }

    !fileInCanonicalDir.getCanonicalFile().equals(fileInCanonicalDir.getAbsoluteFile())
  }

  /**
   * Determines if a directory contains any files newer than cutoff seconds.
   * 确定一个目录是否包含比截止秒更新的任何文件
   * @param dir must be the path to a directory, or IllegalArgumentException is thrown
   * @param cutoff measured in seconds(以秒测量的截止). Returns true if there are any files or directories in the
   *               given directory whose last modified time is later than this many seconds ago
   * 如果在给定目录中的任何文件或目录的最后修改时间比此多秒前的最后一次修改时,则返回真
   */
  def doesDirectoryContainAnyNewFiles(dir: File, cutoff: Long): Boolean = {
    if (!dir.isDirectory) {
      throw new IllegalArgumentException(s"$dir is not a directory!")
    }
    val filesAndDirs = dir.listFiles()
    val cutoffTimeInMillis = System.currentTimeMillis - (cutoff * 1000)

    filesAndDirs.exists(_.lastModified() > cutoffTimeInMillis) ||
    filesAndDirs.filter(_.isDirectory).exists(
      subdir => doesDirectoryContainAnyNewFiles(subdir, cutoff)
    )
  }

  /**
   * Convert a time parameter such as (50s, 100ms, or 250us) to microseconds for internal use. If
   * no suffix is provided, the passed number is assumed to be in ms.
   * 将一个时间参数如（50s,100ms,或250us）以微秒,如果没有任务后缀默认为毫秒
   */
  def timeStringAsMs(str: String): Long = {
    JavaUtils.timeStringAsMs(str)
  }

  /**
   * Convert a time parameter such as (50s, 100ms, or 250us) to microseconds for internal use. If
   * no suffix is provided, the passed number is assumed to be in seconds.
   * 将一个时间参数如（50s,100ms,或250us）以微秒。如果没有提供后缀,默认为秒
   */
  def timeStringAsSeconds(str: String): Long = {
    JavaUtils.timeStringAsSec(str)
  }

  /**
   * Convert a passed byte string (e.g. 50b, 100k, or 250m) to bytes for internal use.
   * 将通过字节字符串（例如50B,100K,或250m）字节,如没有提供后缀默认为byte
   * If no suffix is provided, the passed number is assumed to be in bytes.
   */
  def byteStringAsBytes(str: String): Long = {
    JavaUtils.byteStringAsBytes(str)
  }

  /**
   * Convert a passed byte string (e.g. 50b, 100k, or 250m) to kibibytes for internal use.
   * 将传递的字节串（例如50b，100k或250m）转换为kibibytes以供内部使用。
   * If no suffix is provided, the passed number is assumed to be in kibibytes.
    * 如果没有提供后缀，则传递的数字假定为kibibytes。
   */
  def byteStringAsKb(str: String): Long = {
    JavaUtils.byteStringAsKb(str)
  }

  /**
   * Convert a passed byte string (e.g. 50b, 100k, or 250m) to mebibytes for internal use.
   * 将传递的字节串（例如50b，100k或250m）转换为mebibytes以供内部使用。
   * If no suffix is provided, the passed number is assumed to be in mebibytes.
    * 如果没有提供后缀，则传递的数字假定为mebibytes。
   */
  def byteStringAsMb(str: String): Long = {
    JavaUtils.byteStringAsMb(str)
  }

  /**
   * Convert a passed byte string (e.g. 50b, 100k, or 250m, 500g) to gibibytes for internal use.
   *将传递的字节串（例如50b，100k，或250m，500g）转换为内部使用的gibibytes。
   * If no suffix is provided, the passed number is assumed to be in gibibytes.
    * 如果没有后缀，则传递的数字假定为gibibytes
   */
  def byteStringAsGb(str: String): Long = {
    JavaUtils.byteStringAsGb(str)
  }

  /**
   * 将内存大小字符串转换为以MB为单位的整型值
   * Convert a Java memory parameter passed to -Xmx (such as 300m or 1g) to a number of mebibytes.
   */
  def memoryStringToMb(str: String): Int = {
    // Convert to bytes, rather than directly to MB, because when no units are specified the unit
    // is assumed to be bytes
    //转换为字节,而不是直接转换为MB,因为当没有指定单位时,单位被假定为字节
    (JavaUtils.byteStringAsBytes(str) / 1024 / 1024).toInt
  }

  /**
   * Convert a quantity in bytes to a human-readable string such as "4.0 MB".
   * 将一个字节转换可读的字符串,例如“4.0 MB”
   */
  def bytesToString(size: Long): String = {
    val TB = 1L << 40
    val GB = 1L << 30
    val MB = 1L << 20
    val KB = 1L << 10

    val (value, unit) = {
      if (size >= 2*TB) {
        (size.asInstanceOf[Double] / TB, "TB")
      } else if (size >= 2*GB) {
        (size.asInstanceOf[Double] / GB, "GB")
      } else if (size >= 2*MB) {
        (size.asInstanceOf[Double] / MB, "MB")
      } else if (size >= 2*KB) {
        (size.asInstanceOf[Double] / KB, "KB")
      } else {
        (size.asInstanceOf[Double], "B")
      }
    }
    "%.1f %s".formatLocal(Locale.US, value, unit)
  }

  /**
   * Returns a human-readable string representing a duration such as "35ms"
   * 返回一个人类可读的字符串,表示一个持续时间，如“35ms”
   */
  def msDurationToString(ms: Long): String = {
    val second = 1000
    val minute = 60 * second
    val hour = 60 * minute

    ms match {
      case t if t < second =>
        "%d ms".format(t)
      case t if t < minute =>
        "%.1f s".format(t.toFloat / second)
      case t if t < hour =>
        "%.1f m".format(t.toFloat / minute)
      case t =>
        "%.2f h".format(t.toFloat / hour)
    }
  }

  /**
   * Convert a quantity in megabytes to a human-readable string such as "4.0 MB".
    * 将数量(以兆字节为单位)转换为人为可读的字符串，例如“4.0 MB”
   */
  def megabytesToString(megabytes: Long): String = {
    bytesToString(megabytes * 1024L * 1024L)
  }

  /**
   * Execute a command and return the process running the command.
   * 执行命令并返回运行该命令的进程
   */
  def executeCommand(
      command: Seq[String],
      workingDir: File = new File("."),
      extraEnvironment: Map[String, String] = Map.empty,
      redirectStderr: Boolean = true): Process = {
    //包含程序及其参数的字符串数组,注意Seq数组的传递方式
    val builder = new ProcessBuilder(command: _*).directory(workingDir)
    val environment = builder.environment()
    for ((key, value) <- extraEnvironment) {
      environment.put(key, value)
    }
    val process = builder.start()
    if (redirectStderr) {
      val threadName = "redirect stderr for command " + command(0)
      //读取日志文件
      def log(s: String): Unit = logInfo(s)
      processStreamByLine(threadName, process.getErrorStream, log)
    }
    process
  }

  /**
   * 执行一条command命令,并且获取它的输出,调用stdoutThread的join方法,让当前线程等待stdoutThread执行完成
    * 如果产生非0的代码，则抛出异常。
   * Execute a command and get its output, throwing an exception if it yields a code other than 0.
    * 执行命令并获取其输出,如果产生一个不是0的代码,则抛出异常。
   */
  def executeAndGetOutput(
      command: Seq[String],
      workingDir: File = new File("."),
      extraEnvironment: Map[String, String] = Map.empty,
      redirectStderr: Boolean = true): String = {
    val process = executeCommand(command, workingDir, extraEnvironment, redirectStderr)
    val output = new StringBuffer
    val threadName = "read stdout for " + command(0)
    //读取日志文件
    def appendToOutput(s: String): Unit = output.append(s)
    val stdoutThread = processStreamByLine(threadName, process.getInputStream, appendToOutput)
    val exitCode = process.waitFor()
    //等待它完成读取输出
    stdoutThread.join()   // Wait for it to finish reading output
    if (exitCode != 0) {
      logError(s"Process $command exited with code $exitCode: $output")
      throw new SparkException(s"Process $command exited with code $exitCode")
    }
    output.toString
  }

  /**
   * Return and start a daemon thread that processes the content of the input stream line by line.
    * 返回并启动一行一行处理输入流内容的守护线程。
   */
  def processStreamByLine(
      threadName: String,
      inputStream: InputStream,
      processLine: String => Unit): Thread = {
    val t = new Thread(threadName) {
      override def run() {
        //注意inputStream流的读取方式
        for (line <- Source.fromInputStream(inputStream).getLines()) {
          processLine(line)
        }
      }
    }
    t.setDaemon(true)
    t.start()
    t
  }

  /**
   * Execute a block of code that evaluates to Unit, forwarding any uncaught exceptions to the
   * default UncaughtExceptionHandler
   * 执行一个代码块为单位,转发任何未捕获的异常到默认UncaughtExceptionHandler
   * NOTE: This method is to be called by the spark-started JVM process.
    * 注意：这个方法将由Spark启动的JVM进程调用,
   * 这种方法被称为Spark开始JVM进程
    * block: => T表示函数输入参数为空
   */
  def tryOrExit(block: => Unit) {
    try {
      block
    } catch {
      case e: ControlThrowable => throw e
      case t: Throwable => SparkUncaughtExceptionHandler.uncaughtException(t)
    }
  }

  /**
   * Execute a block of code that evaluates to Unit, stop SparkContext is there is any uncaught
   * exception
   * 执行一个计算单元的代码块,未捕获的异常停止sparkcontext
   * NOTE: This method is to be called by the driver-side components to avoid stopping the
   * user-started JVM process completely; in contrast, tryOrExit is to be called in the
   * spark-started JVM process .
    * 此方法将由驱动端组件调用,以避免停止用户启动JVM进程完全,相反tryorexit被称为Spark启动JVM进程。
   */
  def tryOrStopSparkContext(sc: SparkContext)(block: => Unit) {
    try {
      block
    } catch {
      case e: ControlThrowable => throw e
      case t: Throwable =>
        val currentThreadName = Thread.currentThread().getName
        if (sc != null) {
          logError(s"uncaught error in thread $currentThreadName, stopping SparkContext", t)
          sc.stop()
        }
        if (!NonFatal(t)) {
          logError(s"throw uncaught fatal error in thread $currentThreadName", t)
          throw t
        }
    }
  }

  /**
   * Execute a block of code that evaluates to Unit, re-throwing any non-fatal uncaught
   * exceptions as IOException.  This is used when implementing Externalizable and Serializable's
   * read and write methods, since Java's serializer will not report non-IOExceptions properly;
   * see SPARK-4080 for more context.
   * 执行一个计算单元的代码块,抛出任何非致命的未捕获的异常作为IOException
    *这是在实现Externalizable和序列化的读写方法,因为java的序列化程序将不报告不正确ioexceptions
    * 参数名的调用
   */
  def tryOrIOException(block: => Unit) {
    try {
      block
    } catch {
      case e: IOException => throw e
      case NonFatal(t) => throw new IOException(t)
    }
  }

  /**
   * Execute a block of code that returns a value, re-throwing any non-fatal uncaught
   * exceptions as IOException. This is used when implementing Externalizable and Serializable's
   * read and write methods, since Java's serializer will not report non-IOExceptions properly;
   * see SPARK-4080 for more context.
    * 执行返回值的代码块,将任何非致命的未捕获的异常重新抛出为IOException。
    * 这在实现Externalizable和Serializable的读写方法时使用,
    * 因为Java的串行器不会正确地报告非IOExceptions;有关更多上下文,请参阅SPARK-4080。
   */
  def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException => throw e
      case NonFatal(t) => throw new IOException(t)
    }
  }

  /** 
   *  Executes the given block. Log non-fatal errors if any, and only throw fatal errors 
   *  执行给定的块,如果有任何错误,只抛出致命错误,记录非致命错误
   *  */
  def tryLogNonFatalError(block: => Unit) {
    try {
      block
    } catch {
      case NonFatal(t) =>
        logError(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
    }
  }

  /**
   * Execute a block of code, then a finally block, but if exceptions happen in
   * the finally block, do not suppress the original exception.
   * 执行一个代码块,然后一个finally块,但是如果在finally块中发生异常,抛出原始异常。
   * This is primarily an issue with `finally { out.close() }` blocks, where
   * close needs to be called to clean up `out`, but if an exception happened
   * in `out.write`, it's likely `out` may be corrupted and `out.close` will
   * fail as well. This would then suppress the original/likely more meaningful
   * exception from the original `out.write` call.
   */
  def tryWithSafeFinally[T](block: => T)(finallyBlock: => Unit): T = {
    // It would be nice to find a method on Try that did this
    //这将是很好的找到一种方法,尝试这样做
    var originalThrowable: Throwable = null
    try {
      block
    } catch {
      case t: Throwable =>
        // Purposefully not using NonFatal, because even fatal exceptions
        //故意不使用非致命性的,因为即使是致命的例外
        // we don't want to have our finallyBlock suppress
        //我们不想抑制finallyBlock
        originalThrowable = t
        throw originalThrowable
    } finally {
      try {
        finallyBlock
      } catch {
        case t: Throwable =>
          if (originalThrowable != null) {
            originalThrowable.addSuppressed(t)
            logWarning(s"Suppressing exception in finally: " + t.getMessage, t)
            throw originalThrowable
          } else {
            throw t
          }
      }
    }
  }

  /**
    * Default filtering function for finding call sites using `getCallSite`.
    * 使用“getCallSite”查找调用站点的默认过滤功能。
    * */
  private def sparkInternalExclusionFunction(className: String): Boolean = {
    // A regular expression to match classes of the internal Spark API's
    // that we want to skip when finding the call site of a method.
    //一种正则表达式,用于匹配当查找方法的调用站点时要跳过的内部Spark API的类
    val SPARK_CORE_CLASS_REGEX =
      """^org\.apache\.spark(\.api\.java)?(\.util)?(\.rdd)?(\.broadcast)?\.[A-Z]""".r
    val SPARK_SQL_CLASS_REGEX = """^org\.apache\.spark\.sql.*""".r
    val SCALA_CORE_CLASS_PREFIX = "scala"
    val isSparkClass = SPARK_CORE_CLASS_REGEX.findFirstIn(className).isDefined ||
      SPARK_SQL_CLASS_REGEX.findFirstIn(className).isDefined
    val isScalaClass = className.startsWith(SCALA_CORE_CLASS_PREFIX)
    // If the class is a Spark internal class or a Scala class, then exclude.
    //如果该类是Spark内部类或Scala类,则排除
    isSparkClass || isScalaClass
  }

  /**
   * 获取当前Sparkcontext的当前调用栈的属于Spark或者Scala核心的类压入callStack的栈顶,并将此类的方法lastSparkMethod
   * 将栈里最靠近栈顶的用户类放入callSpark,将此类的行号存入firstUserLine,类名存入firstUserFile
   * When called inside a class in the spark package, returns the name of the user code class
   * (outside the spark package) that called into Spark, as well as which Spark method they called.
   * This is used, for example, to tell users where in their code each RDD got created.
   *
   * @param skipClass Function that is used to exclude non-user-code classes.
   */
  def getCallSite(skipClass: String => Boolean = sparkInternalExclusionFunction): CallSite = {
    // Keep crawling up the stack trace until we find the first function not inside of the spark
    // package. We track the last (shallowest) contiguous Spark method. This might be an RDD
    // transformation, a SparkContext function (such as parallelize), or anything else that leads
    // to instantiation of an RDD. We also track the first (deepest) user method, file, and line.
    var lastSparkMethod = "<unknown>"
    var firstUserFile = "<unknown>"
    var firstUserLine = 0
    var insideSpark = true
    var callStack = new ArrayBuffer[String]() :+ "<unknown>"

    Thread.currentThread.getStackTrace().foreach { ste: StackTraceElement =>
      // When running under some profilers, the current stack trace might contain some bogus
      // frames. This is intended to ensure that we don't crash in these situations by
      // ignoring any frames that we can't examine.
      if (ste != null && ste.getMethodName != null
        && !ste.getMethodName.contains("getStackTrace")) {
        if (insideSpark) {
          if (skipClass(ste.getClassName)) {
            lastSparkMethod = if (ste.getMethodName == "<init>") {
              // Spark method is a constructor; get its class name
              ste.getClassName.substring(ste.getClassName.lastIndexOf('.') + 1)
            } else {
              ste.getMethodName
            }
            callStack(0) = ste.toString // Put last Spark method on top of the stack trace.
          } else {
            firstUserLine = ste.getLineNumber
            firstUserFile = ste.getFileName
            callStack += ste.toString
            insideSpark = false
          }
        } else {
          callStack += ste.toString
        }
      }
    }

    val callStackDepth = System.getProperty("spark.callstack.depth", "20").toInt
    val shortForm =
      if (firstUserFile == "HiveSessionImpl.java") {
        // To be more user friendly, show a nicer string for queries submitted from the JDBC
        // server.为了更加用户友好，为从JDBC服务器提交的查询显示更好的字符串。
        "Spark JDBC Server Query"
      } else {
        s"$lastSparkMethod at $firstUserFile:$firstUserLine"
      }
    val longForm = callStack.take(callStackDepth).mkString("\n")

    CallSite(shortForm, longForm)
  }

  /** 
   *  Return a string containing part of a file from byte 'start' to 'end'. 
   *  返回一个字符串的字节开始到结束的部分文件
   *  */
  def offsetBytes(path: String, start: Long, end: Long): String = {
    val file = new File(path)
    val length = file.length()
    val effectiveEnd = math.min(length, end)
    val effectiveStart = math.max(0, start)
    val buff = new Array[Byte]((effectiveEnd-effectiveStart).toInt)
    val stream = new FileInputStream(file)

    try {
      ByteStreams.skipFully(stream, effectiveStart)
      ByteStreams.readFully(stream, buff)
    } finally {
      stream.close()
    }
    Source.fromBytes(buff).mkString
  }

  /**
   * Return a string containing data across a set of files. The `startIndex`
   * and `endIndex` is based on the cumulative size of all the files take in
   * the given order. See figure below for more details.
    * 在一组文件中返回一个包含数据的字符串,`startIndex`和`endIndex`是基于给定顺序中所有文件的累积大小,有关详细信息，请参见下图。
   */
  def offsetBytes(files: Seq[File], start: Long, end: Long): String = {
    val fileLengths = files.map { _.length }
    val startIndex = math.max(start, 0)
    val endIndex = math.min(end, fileLengths.sum)
    val fileToLength = files.zip(fileLengths).toMap
    logDebug("Log files: \n" + fileToLength.mkString("\n"))

    val stringBuffer = new StringBuffer((endIndex - startIndex).toInt)
    var sum = 0L
    for (file <- files) {
      val startIndexOfFile = sum
      val endIndexOfFile = sum + fileToLength(file)
      logDebug(s"Processing file $file, " +
        s"with start index = $startIndexOfFile, end index = $endIndex")

      /*
                                      ____________
       range 1:                      |            |
                                     |   case A   |

       files:   |==== file 1 ====|====== file 2 ======|===== file 3 =====|

                     |   case B  .       case C       .    case D    |
       range 2:      |___________.____________________.______________|
       */

      if (startIndex <= startIndexOfFile  && endIndex >= endIndexOfFile) {
        // Case C: read the whole file
        //情况C：读取整个文件
        stringBuffer.append(offsetBytes(file.getAbsolutePath, 0, fileToLength(file)))
      } else if (startIndex > startIndexOfFile && startIndex < endIndexOfFile) {
        // Case A and B: read from [start of required range] to [end of file / end of range]
        //情况A和B：从[所需范围的开始]到[文件结束/范围结束]
        val effectiveStartIndex = startIndex - startIndexOfFile
        val effectiveEndIndex = math.min(endIndex - startIndexOfFile, fileToLength(file))
        stringBuffer.append(Utils.offsetBytes(
          file.getAbsolutePath, effectiveStartIndex, effectiveEndIndex))
      } else if (endIndex > startIndexOfFile && endIndex < endIndexOfFile) {
        // Case D: read from [start of file] to [end of require range]
        //案例D：从[文件开始]读取到[要求范围的结束]
        val effectiveStartIndex = math.max(startIndex - startIndexOfFile, 0)
        val effectiveEndIndex = endIndex - startIndexOfFile
        stringBuffer.append(Utils.offsetBytes(
          file.getAbsolutePath, effectiveStartIndex, effectiveEndIndex))
      }
      sum += fileToLength(file)
      logDebug(s"After processing file $file, string built is ${stringBuffer.toString}")
    }
    stringBuffer.toString
  }

  /**
   * Clone an object using a Spark serializer.
    * 使用Spark序列化程序克隆对象。
   */
  def clone[T: ClassTag](value: T, serializer: SerializerInstance): T = {
    serializer.deserialize[T](serializer.serialize(value))
  }

  private def isSpace(c: Char): Boolean = {
    " \t\r\n".indexOf(c) != -1
  }

  /**
   * Split a string of potentially quoted arguments from the command line the way that a shell
   * would do it to determine arguments to a command. For example, if the string is 'a "b c" d',
   * then it would be parsed as three arguments: 'a', 'b c' and 'd'.
    * 从命令行中以shell的方式分割出一系列潜在引用的参数,将它用于确定命令的参数。例如，如果字符串是“A”、“C”、“D”,
    *它将被解析为三个参数：“A”、“B”和“D”。
   */
  def splitCommandString(s: String): Seq[String] = {
    val buf = new ArrayBuffer[String]
    var inWord = false
    var inSingleQuote = false
    var inDoubleQuote = false
    val curWord = new StringBuilder
    def endWord() {
      buf += curWord.toString
      curWord.clear()
    }
    var i = 0
    while (i < s.length) {
      val nextChar = s.charAt(i)
      if (inDoubleQuote) {
        if (nextChar == '"') {
          inDoubleQuote = false
        } else if (nextChar == '\\') {
          if (i < s.length - 1) {
            // Append the next character directly, because only " and \ may be escaped in
            // double quotes after the shell's own expansion
            //直接添加下一个字符，因为只有“和”可以转义 shell扩展后的双引号
            curWord.append(s.charAt(i + 1))
            i += 1
          }
        } else {
          curWord.append(nextChar)
        }
      } else if (inSingleQuote) {
        if (nextChar == '\'') {
          inSingleQuote = false
        } else {
          curWord.append(nextChar)
        }
        // Backslashes are not treated specially in single quotes
        //反斜杠不单引号特殊处理
      } else if (nextChar == '"') {
        inWord = true
        inDoubleQuote = true
      } else if (nextChar == '\'') {
        inWord = true
        inSingleQuote = true
      } else if (!isSpace(nextChar)) {
        curWord.append(nextChar)
        inWord = true
      } else if (inWord && isSpace(nextChar)) {
        endWord()
        inWord = false
      }
      i += 1
    }
    if (inWord || inDoubleQuote || inSingleQuote) {
      endWord()
    }
    buf
  }

 /* Calculates 'x' modulo 'mod', takes to consideration sign of x,
  * i.e. if 'x' is negative, than 'x' % 'mod' is negative too
  * so function return (x % mod) + mod in that case.
  * 计算'x'模'mod',考虑到x的符号,即如果'x'为负,则'x'％'mod'为负,因此在这种情况下函数返回(x％mod)+ mod。
  * 模运算,即求余数
  * Utils.nonNegativeMod(5,2)== 1
  * Utils.nonNegativeMod(23,5)==3
  * Utils.nonNegativeMod(23,5)==3
  */
  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  // Handles idiosyncracies with hash (add more as required)
  // This method should be kept in sync with
  /// /处理的个性与哈希（添加更多的要求）此方法应与
  // org.apache.spark.network.util.JavaUtils#nonNegativeHash().
  def nonNegativeHash(obj: AnyRef): Int = {

    // Required ?
    if (obj eq null) return 0

    val hash = obj.hashCode
    // math.abs fails for Int.MinValue
    //对于Int.MinValue，math.abs失败
    val hashAbs = if (Int.MinValue != hash) math.abs(hash) else 0

    // Nothing else to guard against ?
    hashAbs
  }

  /**
   * NaN-safe version of [[java.lang.Double.compare()]] which allows NaN values to be compared
   * according to semantics where NaN == NaN and NaN > any non-NaN double.
    * NaN安全版本的[[java.lang.Double.compare（）]],允许NaN值根据NaN == NaN和NaN>任何非NaN双的语义进行比较
   */
  def nanSafeCompareDoubles(x: Double, y: Double): Int = {
    val xIsNan: Boolean = java.lang.Double.isNaN(x)
    val yIsNan: Boolean = java.lang.Double.isNaN(y)
    if ((xIsNan && yIsNan) || (x == y)) 0
    else if (xIsNan) 1
    else if (yIsNan) -1
    else if (x > y) 1
    else -1
  }

  /**
   * NaN-safe version of [[java.lang.Float.compare()]] which allows NaN values to be compared
   * according to semantics where NaN == NaN and NaN > any non-NaN float.
    * NaN安全版[[java.lang.Float.compare（）]]，它允许根据NaN == NaN和NaN>任何非NaN float的语义来比较NaN值。
   */
  def nanSafeCompareFloats(x: Float, y: Float): Int = {
    val xIsNan: Boolean = java.lang.Float.isNaN(x)
    val yIsNan: Boolean = java.lang.Float.isNaN(y)
    if ((xIsNan && yIsNan) || (x == y)) 0
    else if (xIsNan) 1
    else if (yIsNan) -1
    else if (x > y) 1
    else -1
  }

  /** 
   *  返回Spark系统属性配置文件
   *  Returns the system properties map that is thread-safe to iterator over. It gets the
    * properties which have been set explicitly, as well as those for which only a default value
    * has been defined.
    *返回线程安全到迭代器的系统属性Map映射,它获取已被明确设置的属性,以及仅定义了默认值的属性
    * */
  def getSystemProperties: Map[String, String] = {
    //System.getProperties()可以确定当前的系统属性,返回值是一个Properties;
    val sysProps = for (key <- System.getProperties.stringPropertyNames()) yield
      (key, System.getProperty(key))

    sysProps.toMap
  }

  /**
   * Method executed for repeating a task for side effects.
   * 执行重复一个副作用的任务的方法
   * Unlike a for comprehension, it permits JVM JIT optimization
    * 与理解不同,它允许JVM JIT优化
   */
  def times(numIters: Int)(f: => Unit): Unit = {
    var i = 0
    while (i < numIters) {
      f
      i += 1
    }
  }

  /**
   * Timing method based on iterations that permit JVM JIT optimization.
   * 基于迭代允许JVM的JIT优化配时方法
   * @param numIters number of iterations 迭代次数
   * @param f function to be executed. If prepare is not None, the running time of each call to f
   *          must be an order of magnitude longer than one millisecond for accurate timing.
    *          功能执行,如果准备不是无,每次调用f的运行时间必须比准确的时间长一个数量级以上,
   * @param prepare function to be executed before each call to f. Its running time doesn't count.
    *                在每次调用f之前执行的功能,它的运行时间不算,
   * @return the total time across all iterations (not couting preparation time)
    *         所有迭代的总时间（不计算准备时间）
   */
  def timeIt(numIters: Int)(f: => Unit, prepare: Option[() => Unit] = None): Long = {
    if (prepare.isEmpty) {
      val start = System.currentTimeMillis
      times(numIters)(f)
      System.currentTimeMillis - start
    } else {
      var i = 0
      var sum = 0L
      while (i < numIters) {
        prepare.get.apply()
        val start = System.currentTimeMillis
        f
        sum += System.currentTimeMillis - start
        i += 1
      }
      sum
    }
  }

  /**
   * 使用循环调用一个迭代器元素数量
   * Counts the number of elements of an iterator using a while loop rather than calling
   * [[scala.collection.Iterator#size]] because it uses a for loop, which is slightly slower
   * in the current version of Scala.
    * 使用while循环计算迭代器的元素数,而不是调用[[scala.collection.Iterator＃size]],
    * 因为它使用了一个for循环.这在当前版本的Scala中稍慢一些。
   */
  def getIteratorSize[T](iterator: Iterator[T]): Long = {
    var count = 0L
    while (iterator.hasNext) {
      count += 1L
      iterator.next()
    }
    count
  }

  /**
   * Creates a symlink. Note jdk1.7 has Files.createSymbolicLink but not used here
   * for jdk1.6 support.  Supports windows by doing copy, everything else uses "ln -sf".
    * 创建符号链接,注意jdk1.7有Files.createSymbolicLink,但这里不支持jdk1.6。
    * 通过复制来支持窗口，其他的都使用“ln -sf”。
   * @param src absolute path to the source
   * @param dst relative path for the destination
   */
  def symlink(src: File, dst: File) {
    if (!src.isAbsolute()) {
      throw new IOException("Source must be absolute")
    }
    if (dst.isAbsolute()) {
      throw new IOException("Destination must be relative")
    }
    var cmdSuffix = ""
    val linkCmd = if (isWindows) {
      // refer to http://technet.microsoft.com/en-us/library/cc771254.aspx
      //请参阅http://technet.microsoft.com/en-us/library/cc771254.aspx
      cmdSuffix = " /s /e /k /h /y /i"
      "cmd /c xcopy "
    } else {
      cmdSuffix = ""
      "ln -sf "
    }
    import scala.sys.process._
    (linkCmd + src.getAbsolutePath() + " " + dst.getPath() + cmdSuffix) lines_!
    ProcessLogger(line => logInfo(line))
  }


  /** 
   *  Return the class name of the given object, removing all dollar signs 
   *  返回给定对象的类名称,删除所有$符号
   *  */
  def getFormattedClassName(obj: AnyRef): String = {
    obj.getClass.getSimpleName.replace("$", "")
  }

  /**
    * Return an option that translates JNothing to None
    * 返回一个选项,将jnothing首屈一指
    * */
  def jsonOption(json: JValue): Option[JValue] = {
    json match {
      case JNothing => None
      case value: JValue => Some(value)
    }
  }

  /** Return an empty JSON object
    *返回空JSON对象
    * */
  def emptyJson: JsonAST.JObject = JObject(List[JField]())

  /**
   * Return a Hadoop FileSystem with the scheme encoded in the given path.
    * 用给定路径编码的scheme返回Hadoop文件系统。
   */
  def getHadoopFileSystem(path: URI, conf: Configuration): FileSystem = {
    FileSystem.get(path, conf)
  }

  /**
   * Return a Hadoop FileSystem with the scheme encoded in the given path.
    * 用给定路径编码的scheme返回Hadoop文件系统
   */
  def getHadoopFileSystem(path: String, conf: Configuration): FileSystem = {
    getHadoopFileSystem(new URI(path), conf)
  }

  /**
   * Return the absolute path of a file in the given directory.
   * 返回给定目录中的文件的绝对路径
   */
  def getFilePath(dir: File, fileName: String): Path = {
    assert(dir.isDirectory)
    val path = new File(dir, fileName).getAbsolutePath
    new Path(path)
  }

  /**
   * Whether the underlying operating system is Windows.
   * 底层的操作系统是否是Windows
   */
  val isWindows = SystemUtils.IS_OS_WINDOWS

  /**
   * Whether the underlying operating system is Mac OS X.
   * 底层的操作系统是否是IOS
   */
  val isMac = SystemUtils.IS_OS_MAC_OSX

  /**
   * Pattern for matching a Windows drive, which contains only a single alphabet character.
   * 匹配Windows驱动器的模式，其中只包含单个字母字符。
   */
  val windowsDrive = "([a-zA-Z])".r

  /**
   * Indicates whether Spark is currently running unit tests.
    *
   */
  def isTesting: Boolean = {
    //System.getenv()和System.getProperties()的区别
    //System.getenv() 返回系统环境变量值 设置系统环境变量：当前登录用户主目录下的".bashrc"文件中可以设置系统环境变量
    //System.getProperties() 返回Java进程变量值 通过命令行参数的"-D"选项
    sys.env.contains("SPARK_TESTING") || sys.props.contains("spark.testing")
  }

  /**
   * Strip the directory from a path name
    * 从路径名中剥离目录
   */
  def stripDirectory(path: String): String = {
    new File(path).getName
  }

  /**
   * Wait for a process to terminate for at most the specified duration.
   * Return whether the process actually terminated after the given timeout.
    * 等待进程终止至多指定的持续时间,返回进程是否在给定超时后终止。
   */
  def waitForProcess(process: Process, timeoutMs: Long): Boolean = {
    var terminated = false
    val startTime = System.currentTimeMillis
    while (!terminated) {
      try {
        process.exitValue()
        terminated = true
      } catch {
        case e: IllegalThreadStateException =>
          // Process not terminated yet
          //进程尚未终止
          if (System.currentTimeMillis - startTime > timeoutMs) {
            return false
          }
          Thread.sleep(100)
      }
    }
    true
  }

  /**
   * Return the stderr of a process after waiting for the process to terminate.
   * If the process does not terminate within the specified timeout, return None.
   * 等待进程结束后返回一个进程的标准错误,如果进程在指定的超时时间内不终止,返回None。
   */
  def getStderr(process: Process, timeoutMs: Long): Option[String] = {
    val terminated = Utils.waitForProcess(process, timeoutMs)
    if (terminated) {
      Some(Source.fromInputStream(process.getErrorStream).getLines().mkString("\n"))
    } else {
      None
    }
  }

  /**
   * Execute the given block, logging and re-throwing any uncaught exception. 
   * This is particularly useful for wrapping code that runs in a thread, to ensure
   * that exceptions are printed, and to avoid having to catch Throwable.
    * 执行给定的块,记录并重新抛出任何未捕获的异常。这对于包装在线程中运行的代码特别有用，以确保打印出异常，并避免不得不捕获Throwable。
   */
  def logUncaughtExceptions[T](f: => T): T = {
    try {
      f
    } catch {
      case ct: ControlThrowable =>
        throw ct
      case t: Throwable =>
        logError(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
        throw t
    }
  }

  /** 
   *  Executes the given block in a Try, logging any uncaught exceptions.
   *  给定的块中尝试执行,记录任何未捕获的异常
   *   */
  def tryLog[T](f: => T): Try[T] = {
    try {
      val res = f
      scala.util.Success(res)
    } catch {
      case ct: ControlThrowable =>
        throw ct
      case t: Throwable =>
        logError(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
        scala.util.Failure(t)
    }
  }

  /** 
   *  Returns true if the given exception was fatal. See docs for scala.util.control.NonFatal. 
   *  如果给定的异常是致命的,则返回真
   *  */
  def isFatalError(e: Throwable): Boolean = {
    e match {
      case NonFatal(_) | _: InterruptedException | _: NotImplementedError | _: ControlThrowable =>
        false
      case _ =>
        true
    }
  }
  //解释器

  lazy val isInInterpreter: Boolean = {
    try {
      val interpClass = classForName("org.apache.spark.repl.Main")
      interpClass.getMethod("interp").invoke(null) != null
    } catch {
      case _: ClassNotFoundException => false
    }
  }

  /**
   * Return a well-formed URI for the file described by a user input string.
   * 返回用户输入字符串描述的文件的格式良好的URI
   * If the supplied path does not contain a scheme, or is a relative path, it will be
   * converted into an absolute path with a file:// scheme.
    * 如果提供的路径不包含方案,或者是相对路径,则将是使用file：//方案转换为绝对路径
    *spark.jar=====file:/spark.jar
    * hdfs:///root/spark.jar#app.jar=====hdfs:/root/spark.jar#app.jar
    * file:///C:/path/to/file.txt=====file:/C:/path/to/file.txt
   */
  def resolveURI(path: String): URI = {
    try {
      val uri = new URI(path)
      if (uri.getScheme() != null) {
        return uri
      }
      // make sure to handle if the path has a fragment (applies to yarn
      // distributed cache)
      //确保处理路径有片段（适用于YAR分布式缓存）
      //获取Uri中的Fragment部分,即harvic
      if (uri.getFragment() != null) {
        val absoluteURI = new File(uri.getPath()).getAbsoluteFile().toURI()
        return new URI(absoluteURI.getScheme(), absoluteURI.getHost(), absoluteURI.getPath(),
          uri.getFragment())
      }
    } catch {
      case e: URISyntaxException =>
    }
    new File(path).getAbsoluteFile().toURI()
  }

  /** 
   *  Resolve a comma-separated list of paths. 
   *  解析一个逗号分隔的路径列表
    *  jar1,jar2=====file:/jar1,file:/jar2
    *  hdfs:/jar1,file:/jar2,jar3=====hdfs:/jar1,file:/jar2,file:/jar3
   *  */
  def resolveURIs(paths: String): String = {
    if (paths == null || paths.trim.isEmpty) {
      ""
    } else {
      paths.split(",").map { p => Utils.resolveURI(p) }.mkString(",")
    }
  }

  /** 
   *  Return all non-local paths from a comma-separated list of paths. 
   *  返回从逗号分隔的路径列表中的所有非本地路径。
   *  */
  def nonLocalPaths(paths: String, testWindows: Boolean = false): Array[String] = {
    val windows = isWindows || testWindows
    if (paths == null || paths.trim.isEmpty) {
      Array.empty
    } else {
      paths.split(",").filter { p =>
        val uri = resolveURI(p)
        Option(uri.getScheme).getOrElse("file") match {
          case windowsDrive(d) if windows => false
          case "local" | "file" => false
          case _ => true
        }
      }
    }
  }

  /**
   * 加载指定文件中的Spark属性,如果没有指定文件,默认Spark属性文件的属性
   * Load default Spark properties from the given file. If no file is provided,
   * use the common defaults file. This mutates state in the given SparkConf and
   * in this JVM's system properties if the config specified in the file is not
   * already set. Return the path of the properties file used.
    * 从给定文件加载默认Spark属性,如果没有提供文件,请使用通用的默认文件。
    * 如果文件中指定的配置尚未设置,则会在给定的SparkConf和该JVM的系统属性中突出显示状态。
    * 返回所使用的属性文件的路径。
   */
  def loadDefaultSparkProperties(conf: SparkConf, filePath: String = null): String = {
    val path = Option(filePath).getOrElse(getDefaultPropertiesFile())
    Option(path).foreach { confFile =>
      getPropertiesFromFile(confFile).filter { case (k, v) =>
        k.startsWith("spark.")
      }.foreach { case (k, v) =>
        conf.setIfMissing(k, v)
        //System.getenv()和System.getProperties()的区别
        //System.getenv() 返回系统环境变量值 设置系统环境变量：当前登录用户主目录下的".bashrc"文件中可以设置系统环境变量
        //System.getProperties() 返回Java进程变量值 通过命令行参数的"-D"选项
        sys.props.getOrElseUpdate(k, v)
      }
    }
    path
  }

  /** 
   *  Load properties present in the given file. 
   *  加载给定文件中的属性
   *  */
  def getPropertiesFromFile(filename: String): Map[String, String] = {
    val file = new File(filename)
    require(file.exists(), s"Properties file $file does not exist")
    require(file.isFile(), s"Properties file $file is not a normal file")

    val inReader = new InputStreamReader(new FileInputStream(file), "UTF-8")
    try {
      val properties = new Properties()
      properties.load(inReader)
      //stringPropertyNames 返回此属性列表中的一组键,键对应的值是字符串,properties(k).trim取出的是值
      properties.stringPropertyNames().map(k => (k, properties(k).trim)).toMap
    } catch {
      case e: IOException =>
        throw new SparkException(s"Failed when loading Spark properties from $filename", e)
    } finally {
      inReader.close()
    }
  }

  /** 
   *  Return the path of the default Spark properties file.
   *  返回默认的Spark属性文件的路径
    *
   *   */
  //System.getenv()和System.getProperties()的区别
  //System.getenv() 返回系统环境变量值 设置系统环境变量：当前登录用户主目录下的".bashrc"文件中可以设置系统环境变量
  //System.getProperties() 返回Java进程变量值 通过命令行参数的"-D"选项
  def getDefaultPropertiesFile(env: Map[String, String] = sys.env): String = {
    env.get("SPARK_CONF_DIR")
      .orElse(env.get("SPARK_HOME").map { t => s"$t${File.separator}conf" })
      .map { t => new File(s"$t${File.separator}spark-defaults.conf")}
      .filter(_.isFile)
      .map(_.getAbsolutePath)
      .orNull
  }

  /**
   * Return a nice string representation of the exception. It will call "printStackTrace" to
   * recursively generate the stack trace including the exception and its causes.
   * 返回异常的一个很好的字符串表示形式。 它会调用“printStackTrace”递归生成堆栈跟踪，包括异常及其原因。
   */
  def exceptionString(e: Throwable): String = {
    if (e == null) {
      ""
    } else {
      // Use e.printStackTrace here because e.getStackTrace doesn't include the cause
      //在这里使用e.printStackTrace，因为e.getStackTrace不包括原因
      val stringWriter = new StringWriter()
      e.printStackTrace(new PrintWriter(stringWriter))
      stringWriter.toString
    }
  }

  /** 
   *  Return a thread dump of all threads' stacktraces.  Used to capture dumps for the web UI
   *  返回所有线程的堆栈跟踪线程,用于捕获Web用户界面
   *   */
  def getThreadDump(): Array[ThreadStackTrace] = {
    // We need to filter out null values here because dumpAllThreads() may return null array
    // elements for threads that are dead / don't exist.
    //我们需要在这里过滤出空值,因为dumpAllThreads()可能会返回null数组元素,因为死线程/不存在的线程。
    val threadInfos = ManagementFactory.getThreadMXBean.dumpAllThreads(true, true).filter(_ != null)
    threadInfos.sortBy(_.getThreadId).map { case threadInfo =>
      val stackTrace = threadInfo.getStackTrace.map(_.toString).mkString("\n")
      ThreadStackTrace(threadInfo.getThreadId, threadInfo.getThreadName,
        threadInfo.getThreadState, stackTrace)
    }
  }

  /**
   * Convert all spark properties set in the given SparkConf to a sequence of java options.
   * 将所有的Spark在给定的sparkconf属性设置为序列java选项
   */
  def sparkJavaOpts(conf: SparkConf, filterKey: (String => Boolean) = _ => true): Seq[String] = {
    conf.getAll
      .filter { case (k, _) => filterKey(k) }
      .map { case (k, v) => s"-D$k=$v" }
  }

  /**
   * Maximum number of retries when binding to a port before giving up.
   * 最大重试次数时,绑定到一个端口
   */
  def portMaxRetries(conf: SparkConf): Int = {
    val maxRetries = conf.getOption("spark.port.maxRetries").map(_.toInt)
    if (conf.contains("spark.testing")) {
      // Set a higher number of retries for tests...
      //设置更多的重试次数以进行测试...
      maxRetries.getOrElse(100)
    } else {
      maxRetries.getOrElse(16)
    }
  }

  /**
   * Scala跟其他脚本语言一样,函数可以传递,此方法正是通过回调StartService这个函数来启动服务,
   * 并最终返回startService返回的service地址及端口,如果启动过程有异常,还会多次重试,直到达 到maxRetries表示的最大次数
   * Attempt to start a service on the given port, or fail after a number of attempts.
    * 尝试在给定端口上启动服务,或者经过多次尝试后失败
   * Each subsequent attempt uses 1 + the port used in the previous attempt (unless the port is 0).
   * 每次后续尝试使用1 +上次尝试中使用的端口(除非端口为0)。
   * @param startPort The initial port to start the service on. 启动服务的初始端口
   * @param startService Function to start service on a given port. 在给定端口上启动服务的功能
   *                     This is expected to throw java.net.BindException on port collision.
   * @param conf A SparkConf used to get the maximum number of retries when binding to a port.
    *            用于在绑定到端口时获取最大重试次数的SparkConf
   * @param serviceName Name of the service. 服务名称
   */
  def startServiceOnPort[T](
      startPort: Int,
      startService: Int => (T, Int),
      conf: SparkConf,
      serviceName: String = ""): (T, Int) = {

    require(startPort == 0 || (1024 <= startPort && startPort < 65536),
      "startPort should be between 1024 and 65535 (inclusive), or 0 for a random free port.")

    val serviceString = if (serviceName.isEmpty) "" else s" '$serviceName'"
    val maxRetries = portMaxRetries(conf)
    for (offset <- 0 to maxRetries) {
      // Do not increment port if startPort is 0, which is treated as a special port
      // 如果startPort为0，则不要递增端口,这被视为特殊端口
      val tryPort = if (startPort == 0) {
        startPort
      } else {
        // If the new port wraps around, do not try a privilege port
        // 如果新端口包围,请勿尝试特权端口
        ((startPort + offset - 1024) % (65536 - 1024)) + 1024
      }
      try {
        val (service, port) = startService(tryPort)
        logInfo(s"Successfully started service$serviceString on port $port.")
        return (service, port)
      } catch {
        case e: Exception if isBindCollision(e) =>
          if (offset >= maxRetries) {
            val exceptionMessage =
              s"${e.getMessage}: Service$serviceString failed after $maxRetries retries!"
            val exception = new BindException(exceptionMessage)
            // restore original stack trace
            // 恢复原始堆栈跟踪
            exception.setStackTrace(e.getStackTrace)
            throw exception
          }
          logWarning(s"Service$serviceString could not bind on port $tryPort. " +
            s"Attempting port ${tryPort + 1}.")
      }
    }
    // Should never happen
    throw new SparkException(s"Failed to start service$serviceString on port $startPort")
  }

  /**
   * Return whether the exception is caused by an address-port collision when binding.
   * 返回是否绑定时的地址端口造成的异常
   */
  def isBindCollision(exception: Throwable): Boolean = {
    exception match {
      case e: BindException =>
        if (e.getMessage != null) {
          return true
        }
        isBindCollision(e.getCause)
      case e: MultiException => e.getThrowables.exists(isBindCollision)
      case e: Exception => isBindCollision(e.getCause)
      case _ => false
    }
  }

  /**
   * configure a new log4j level
   * 设置一个新log4j级别
   */
  def setLogLevel(l: org.apache.log4j.Level) {
    org.apache.log4j.Logger.getRootLogger().setLevel(l)
  }

  /**
   * config a log4j properties used for testsuite
    * 配置用于testsuite的log4j属性
   */
  def configTestLog4j(level: String): Unit = {
    val pro = new Properties()
    pro.put("log4j.rootLogger", s"$level, console")
    pro.put("log4j.appender.console", "org.apache.log4j.ConsoleAppender")
    pro.put("log4j.appender.console.target", "System.err")
    pro.put("log4j.appender.console.layout", "org.apache.log4j.PatternLayout")
    pro.put("log4j.appender.console.layout.ConversionPattern",
      "%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n")
    PropertyConfigurator.configure(pro)
  }

  /**
   * If the given URL connection is HttpsURLConnection, it sets the SSL socket factory and
   * the host verifier from the given security manager.
    *如果给定的URL连接是HttpsURLConnection，它将设置SSL套接字工厂和来自给定安全管理员的主机验证者
   */
  def setupSecureURLConnection(urlConnection: URLConnection, sm: SecurityManager): URLConnection = {
    urlConnection match {
      case https: HttpsURLConnection =>
        sm.sslSocketFactory.foreach(https.setSSLSocketFactory)
        sm.hostnameVerifier.foreach(https.setHostnameVerifier)
        https
      case connection => connection
    }
  }

  def invoke(
      clazz: Class[_],
      obj: AnyRef,
      methodName: String,
      args: (Class[_], AnyRef)*): AnyRef = {
    val (types, values) = args.unzip
    val method = clazz.getDeclaredMethod(methodName, types: _*)
    method.setAccessible(true)
    method.invoke(obj, values.toSeq: _*)
  }

  // Limit of bytes for total size of results (default is 1GB)
  //如果结果的大小大于1GB,那么直接丢弃,
   // 可以在spark.driver.maxResultSize设置
  def getMaxResultSize(conf: SparkConf): Long = {
    memoryStringToMb(conf.get("spark.driver.maxResultSize", "1g")).toLong << 20
  }

  /**
   * Return the current system LD_LIBRARY_PATH name
   * 返回当前系统路径名
   */
  def libraryPathEnvName: String = {
    if (isWindows) {
      "PATH"
    } else if (isMac) {
      "DYLD_LIBRARY_PATH"
    } else {
      "LD_LIBRARY_PATH"
    }
  }

  /**
   * Return the prefix of a command that appends the given library paths to the
   * system-specific library path environment variable. On Unix, for instance,
   * this returns the string LD_LIBRARY_PATH="path1:path2:$LD_LIBRARY_PATH".
    * 返回将给定库路径附加到系统特定库路径环境变量的命令的前缀。
    * 例如,在Unix上,这将返回字符串LD_LIBRARY_PATH =“path1：path2：$ LD_LIBRARY_PATH”。
   */
  def libraryPathEnvPrefix(libraryPaths: Seq[String]): String = {
    val libraryPathScriptVar = if (isWindows) {
      s"%${libraryPathEnvName}%"
    } else {
      "$" + libraryPathEnvName
    }
    val libraryPath = (libraryPaths :+ libraryPathScriptVar).mkString("\"",
      File.pathSeparator, "\"")
    val ampersand = if (Utils.isWindows) {
      " &"
    } else {
      ""
    }
    s"$libraryPathEnvName=$libraryPath$ampersand"
  }

  /**
   * Return the value of a config either through the SparkConf or the Hadoop configuration
   * if this is Yarn mode. In the latter case, this defaults to the value set through SparkConf
   * if the key is not set in the Hadoop configuration.
    * 如果是Yarn模式,则通过SparkConf或Hadoop配置返回配置值,在后一种情况下,
    * 如果在Hadoop配置中未设置Key,则此值将默认为通过SparkConf设置的值。
   */
  def getSparkOrYarnConfig(conf: SparkConf, key: String, default: String): String = {
    val sparkValue = conf.get(key, default)
    if (SparkHadoopUtil.get.isYarnMode) {
      SparkHadoopUtil.get.newConfiguration(conf).get(key, sparkValue)
    } else {
      sparkValue
    }
  }

  /**
   * Return a pair of host and port extracted from the `sparkUrl`.
   * 返回一个从`sparkUrl`提取的主机和端口。
   * A spark url (`spark://host:port`) is a special URI that its scheme is `spark` and only contains
   * host and port.
    * 一个spark网址（`spark：// host：port`）是一个特殊的URI,它的方案是“spark”,只包含主机和端口。
   *
   * @throws SparkException if `sparkUrl` is invalid.
   */
  def extractHostPortFromSparkUrl(sparkUrl: String): (String, Int) = {
    try {
      val uri = new java.net.URI(sparkUrl)
      val host = uri.getHost
      val port = uri.getPort
      if (uri.getScheme != "spark" ||
        host == null ||
        port < 0 ||
        //uri.getPath返回“”而不是null
        (uri.getPath != null && !uri.getPath.isEmpty) || // uri.getPath returns "" instead of null
        uri.getFragment != null ||
        uri.getQuery != null ||
        uri.getUserInfo != null) {
        throw new SparkException("Invalid master URL: " + sparkUrl)
      }
      (host, port)
    } catch {
      case e: java.net.URISyntaxException =>
        throw new SparkException("Invalid master URL: " + sparkUrl, e)
    }
  }

  /**
   * Returns the current user name. This is the currently logged in user, unless that's been
   * overridden by the `SPARK_USER` environment variable.
    * 返回当前用户名,这是当前登录的用户,除非已经登录被“SPARK_USER”环境变量所覆盖。
   */
  def getCurrentUserName(): String = {
    Option(System.getenv("SPARK_USER"))
      .getOrElse(UserGroupInformation.getCurrentUser().getShortUserName())
  }

  /**
   * Split the comma delimited string of master URLs into a list.
   * 将Master目录以逗号分隔成一个字符串列表。
   * For instance, "spark://abc,def" becomes [spark://abc, spark://def].
   */
  def parseStandaloneMasterUrls(masterUrls: String): Array[String] = {
    masterUrls.stripPrefix("spark://").split(",").map("spark://" + _)
  }

  /** 
   *  An identifier that backup masters use in their responses.
   *  代表备份Master标识符
   *  */
  val BACKUP_STANDALONE_MASTER_PREFIX = "Current state is not alive"

  /** 
   *  Return true if the response message is sent from a backup Master on standby.
   *  如果响应消息从备用主发送的响应消息,则返回.
   *   */
  def responseFromBackup(msg: String): Boolean = {
    msg.startsWith(BACKUP_STANDALONE_MASTER_PREFIX)
  }

  /**
   * To avoid calling `Utils.getCallSite` for every single RDD we create in the body,
   * set a dummy call site that RDDs use instead. This is for performance optimization.
    * 为了避免在身体中创建的每个RDD调用“Utils.getCallSite”,设置RDD使用的虚拟调用站点,这是为了性能优化。
   */
  def withDummyCallSite[T](sc: SparkContext)(body: => T): T = {
    val oldShortCallSite = sc.getLocalProperty(CallSite.SHORT_FORM)
    val oldLongCallSite = sc.getLocalProperty(CallSite.LONG_FORM)
    try {
      sc.setLocalProperty(CallSite.SHORT_FORM, "")
      sc.setLocalProperty(CallSite.LONG_FORM, "")
      body
    } finally {
      // Restore the old ones here
      //在这里恢复旧的
      sc.setLocalProperty(CallSite.SHORT_FORM, oldShortCallSite)
      sc.setLocalProperty(CallSite.LONG_FORM, oldLongCallSite)
    }
  }

  /**
   * Return whether the specified file is a parent directory of the child file.
   * 返回指定的文件是否是子文件的父目录
   */
  def isInDirectory(parent: File, child: File): Boolean = {
    if (child == null || parent == null) {
      return false
    }
    if (!child.exists() || !parent.exists() || !parent.isDirectory()) {
      return false
    }
    if (parent.equals(child)) {
      return true
    }
    isInDirectory(parent, child.getParentFile)
  }

  /**
   * Return whether dynamic allocation is enabled in the given conf
   * Dynamic allocation and explicitly setting the number of executors are inherently
   * incompatible. In environments where dynamic allocation is turned on by default,
   * the latter should override the former (SPARK-9092).
    * 返回是否在给定的conf中启用了动态分配动态分配和明确设置执行程序的数量本质上是不兼容的,在默认情况下打开动态分配的环境中
   */
  def isDynamicAllocationEnabled(conf: SparkConf): Boolean = {
    conf.getBoolean("spark.dynamicAllocation.enabled", false) &&
      conf.getInt("spark.executor.instances", 0) == 0
  }

  /**
   * Returns a path of temporary file which is in the same directory with `path`.
   * 返回一个临时文件的路径,它位于同一个目录中的“路径”。
   */
  def tempFileWith(path: File): File = {
    new File(path.getAbsolutePath + "." + UUID.randomUUID())
  }
}

/**
 * A utility class to redirect the child process's stdout or stderr.
 * 一个工具类重定向子进程的stdout和stderr。
 */
private[spark] class RedirectThread(
    in: InputStream,
    out: OutputStream,
    name: String,
    propagateEof: Boolean = false)
  extends Thread(name) {

  setDaemon(true)
  override def run() {
    scala.util.control.Exception.ignoring(classOf[IOException]) {
      // FIXME: We copy the stream on the level of bytes to avoid encoding problems.
      Utils.tryWithSafeFinally {
        val buf = new Array[Byte](1024)
        var len = in.read(buf)
        while (len != -1) {
          out.write(buf, 0, len)
          out.flush()
          len = in.read(buf)
        }
      } {
        if (propagateEof) {
          out.close()
        }
      }
    }
  }
}

/**
 * An [[OutputStream]] that will store the last 10 kilobytes (by default) written to it
 * in a circular buffer. The current contents of the buffer can be accessed using
 * the toString method.
  * 一个[[OutputStream]]，将存储写入它的最后10千字节（默认）在循环缓冲区,缓冲区的当前内容可以使用toString方法。
 */
private[spark] class CircularBuffer(sizeInBytes: Int = 10240) extends java.io.OutputStream {
  var pos: Int = 0
  var buffer = new Array[Int](sizeInBytes)

  def write(i: Int): Unit = {
    buffer(pos) = i
    pos = (pos + 1) % buffer.length
  }

  override def toString: String = {
    val (end, start) = buffer.splitAt(pos)
    val input = new java.io.InputStream {
      val iterator = (start ++ end).iterator

      def read(): Int = if (iterator.hasNext) iterator.next() else -1
    }
    val reader = new BufferedReader(new InputStreamReader(input))
    val stringBuilder = new StringBuilder
    var line = reader.readLine()
    while (line != null) {
      stringBuilder.append(line)
      stringBuilder.append("\n")
      line = reader.readLine()
    }
    stringBuilder.toString()
  }
}
