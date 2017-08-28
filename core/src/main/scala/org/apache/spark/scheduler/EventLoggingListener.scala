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

package org.apache.spark.scheduler

import java.io._
import java.net.URI

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.google.common.base.Charsets
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataOutputStream, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods._

import org.apache.spark.{Logging, SparkConf, SPARK_VERSION}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.{JsonProtocol, Utils}

/**
 * A SparkListener that logs events to persistent storage.
 * 一个Spark监听器事件将日志存储
 * Event logging is specified by the following configurable parameters:
  * 事件记录由以下可配置参数指定：
 *   spark.eventLog.enabled - Whether event logging is enabled.是否启用事件日志记录
 *   spark.eventLog.compress - Whether to compress logged events 是否压缩记录的事件
 *   spark.eventLog.overwrite - Whether to overwrite any existing files.是否覆盖任何现有的文件
 *   spark.eventLog.dir - Path to the directory in which events are logged.记录事件的目录的路径
 *   spark.eventLog.buffer.kb - Buffer size to use when writing to output streams写入输出流时使用的缓冲区大小
 */
private[spark] class EventLoggingListener(
    appId: String,
    appAttemptId : Option[String],
    logBaseDir: URI,
    sparkConf: SparkConf,
    hadoopConf: Configuration)
  extends SparkListener with Logging {

  import EventLoggingListener._

  def this(appId: String, appAttemptId : Option[String], logBaseDir: URI, sparkConf: SparkConf) =
    this(appId, appAttemptId, logBaseDir, sparkConf,
      SparkHadoopUtil.get.newConfiguration(sparkConf))
  //是否压缩记录Spark事件,前提spark.eventLog.enabled为true
  private val shouldCompress = sparkConf.getBoolean("spark.eventLog.compress", false)
   //是否覆盖Spark事件日志文件,前提spark.eventLog.enabled为true
  private val shouldOverwrite = sparkConf.getBoolean("spark.eventLog.overwrite", false)
  private val testing = sparkConf.getBoolean("spark.eventLog.testing", false)
  //Spark日志的输出大小
  private val outputBufferSize = sparkConf.getInt("spark.eventLog.buffer.kb", 100) * 1024
  private val fileSystem = Utils.getHadoopFileSystem(logBaseDir, hadoopConf)
  private val compressionCodec =
    if (shouldCompress) {
      Some(CompressionCodec.createCodec(sparkConf))
    } else {
      None
    }
  private val compressionCodecName = compressionCodec.map { c =>
    CompressionCodec.getShortName(c.getClass.getName)
  }

  // Only defined if the file system scheme is not local
  // 定义一个不是本地的文件系统
  private var hadoopDataStream: Option[FSDataOutputStream] = None

  // The Hadoop APIs have changed over time, so we use reflection to figure out
  // the correct method to use to flush a hadoop data stream. See SPARK-1518
  // for details.
  //Hadoop API随着时间的推移而改变,所以我们使用反射来找出用于刷新hadoop数据流的正确方法,详见SPARK-1518。
  private val hadoopFlushMethod = {
    val cls = classOf[FSDataOutputStream]
    scala.util.Try(cls.getMethod("hflush")).getOrElse(cls.getMethod("sync"))
  }

  private var writer: Option[PrintWriter] = None

  // For testing. Keep track of all JSON serialized events that have been logged.
  //测试,跟踪所有的JSON序列化已记录的事件
  private[scheduler] val loggedEvents = new ArrayBuffer[JValue]

  // Visible for tests only.
  //可见仅用于测试
  private[scheduler] val logPath = getLogPath(logBaseDir, appId, appAttemptId, compressionCodecName)

  /**
   * Creates the log file in the configured log directory.
   * 在已配置的日志目录中创建日志文件
   */
  def start() {
    if (!fileSystem.getFileStatus(new Path(logBaseDir)).isDir) {
      throw new IllegalArgumentException(s"Log directory $logBaseDir does not exist.")
    }

    val workingPath = logPath + IN_PROGRESS
    val uri = new URI(workingPath)
    val path = new Path(workingPath)
    val defaultFs = FileSystem.getDefaultUri(hadoopConf).getScheme
    val isDefaultLocal = defaultFs == null || defaultFs == "file"

    if (shouldOverwrite && fileSystem.exists(path)) {
      logWarning(s"Event log $path already exists. Overwriting...")
      fileSystem.delete(path, true)
    }

    /* The Hadoop LocalFileSystem (r1.0.4) has known issues with syncing (HADOOP-7844).
     * Therefore, for local files, use FileOutputStream instead.
     * Hadoop LocalFileSystem(r1.0.4)已知同步问题（HADOOP-7844,因此,对于本地文件,请改用FileOutputStream。*/
    val dstream =
      if ((isDefaultLocal && uri.getScheme == null) || uri.getScheme == "file") {
        new FileOutputStream(uri.getPath)
      } else {
        hadoopDataStream = Some(fileSystem.create(path))
        hadoopDataStream.get
      }

      try {
      val cstream = compressionCodec.map(_.compressedOutputStream(dstream)).getOrElse(dstream)
      val bstream = new BufferedOutputStream(cstream, outputBufferSize)

      EventLoggingListener.initEventLog(bstream)
      fileSystem.setPermission(path, LOG_FILE_PERMISSIONS)
      writer = Some(new PrintWriter(bstream))
      logInfo("Logging events to %s".format(logPath))
    } catch {
      case e: Exception =>
        dstream.close()
        throw e
    }
  }

  /** Log the event as JSON.
    * 将事件记录为JSON*/
  private def logEvent(event: SparkListenerEvent, flushLogger: Boolean = false) {
    val eventJson = JsonProtocol.sparkEventToJson(event)
    // scalastyle:off println
    writer.foreach(_.println(compact(render(eventJson))))
    // scalastyle:on println
    if (flushLogger) {
      writer.foreach(_.flush())
      hadoopDataStream.foreach(hadoopFlushMethod.invoke(_))
    }
    if (testing) {
      loggedEvents += eventJson
    }
  }

  // Events that do not trigger a flush 不触发flush的事件
  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = logEvent(event)

  override def onTaskStart(event: SparkListenerTaskStart): Unit = logEvent(event)

  override def onTaskGettingResult(event: SparkListenerTaskGettingResult): Unit = logEvent(event)

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = logEvent(event)

  override def onEnvironmentUpdate(event: SparkListenerEnvironmentUpdate): Unit = logEvent(event)

  // Events that trigger a flush 触发flush的事件
  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onJobStart(event: SparkListenerJobStart): Unit = logEvent(event, flushLogger = true)

  override def onJobEnd(event: SparkListenerJobEnd): Unit = logEvent(event, flushLogger = true)

  override def onBlockManagerAdded(event: SparkListenerBlockManagerAdded): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onBlockManagerRemoved(event: SparkListenerBlockManagerRemoved): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onUnpersistRDD(event: SparkListenerUnpersistRDD): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onApplicationStart(event: SparkListenerApplicationStart): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = {
    logEvent(event, flushLogger = true)
  }
  override def onExecutorAdded(event: SparkListenerExecutorAdded): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit = {
    logEvent(event, flushLogger = true)
  }

  // No-op because logging every update would be overkill 无操作,因为记录每个更新将是过度的
  override def onBlockUpdated(event: SparkListenerBlockUpdated): Unit = {}

  // No-op because logging every update would be overkill 无操作,因为记录每个更新将是过度的
  override def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Unit = { }

  /**
   * Stop logging events. The event log file will be renamed so that it loses the
   * ".inprogress" suffix.
    * 停止记录事件,事件日志文件将被重命名,以便丢失“.inprogress”后缀,
   */
  def stop(): Unit = {
    writer.foreach(_.close())

    val target = new Path(logPath)
    if (fileSystem.exists(target)) {
      if (shouldOverwrite) {
        logWarning(s"Event log $target already exists. Overwriting...")
        fileSystem.delete(target, true)
      } else {
        throw new IOException("Target log file already exists (%s)".format(logPath))
      }
    }
    fileSystem.rename(new Path(logPath + IN_PROGRESS), target)
  }

}

private[spark] object EventLoggingListener extends Logging {
  // Suffix applied to the names of files still being written by applications.
  //后缀应用于仍由应用程序编写的文件的名称
  //后缀的文件名称
  val IN_PROGRESS = ".inprogress"
  val DEFAULT_LOG_DIR = "/tmp/spark-events"
  val SPARK_VERSION_KEY = "SPARK_VERSION"
  val COMPRESSION_CODEC_KEY = "COMPRESSION_CODEC"

  private val LOG_FILE_PERMISSIONS = new FsPermission(Integer.parseInt("770", 8).toShort)

  // A cache for compression codecs to avoid creating the same codec many times
  //用于压缩编解码器的缓存,以避免创建相同的编解码器多次
  private val codecMap = new mutable.HashMap[String, CompressionCodec]

  /**
   * Write metadata about an event log to the given stream.
   * The metadata is encoded in the first line of the event log as JSON.
   * 将事件日志的元数据写入到输出中,元数据编码在事件日志中的第一行作为JSON
   * @param logStream Raw output stream to the event log file.
   */
  def initEventLog(logStream: OutputStream): Unit = {
    val metadata = SparkListenerLogStart(SPARK_VERSION)
    val metadataJson = compact(JsonProtocol.logStartToJson(metadata)) + "\n"
    logStream.write(metadataJson.getBytes(Charsets.UTF_8))
  }

  /**
   * Return a file-system-safe path to the log file for the given application.
   * 将文件系统安全路径返回给给定应用程序的日志文件
    *
   * Note that because we currently only create a single log file for each application,
   * we must encode all the information needed to parse this event log in the file name
   * instead of within the file itself. Otherwise, if the file is compressed, for instance,
   * we won't know which codec to use to decompress the metadata needed to open the file in
   * the first place.
    *
    * 请注意,因为我们目前只为每个应用程序创建一个单一的日志文件,所以我们必须对文件名中的所有信息进行编码,而不是在文件本身内解析此事件日志。
    * 否则,如果文件被压缩,例如,我们将不知道使用哪个编解码器解压缩首先打开文件栈所需的元数据。
   *
   * The log file name will identify the compression codec used for the contents, if any.
   * For example, app_123 for an uncompressed log, app_123.lzf for an LZF-compressed log.
    *
    * 日志文件名称将标识用于内容的压缩编解码器(如果有的话),例如,对于未压缩日志的app_123,LZF压缩日志的app_123.lzf。
   *
   * @param logBaseDir Directory where the log file will be written.要写入日志文件的目录
   * @param appId A unique app ID.独特的应用程式编号
   * @param appAttemptId A unique attempt id of appId. May be the empty string.appId的唯一尝试ID,可能是空字符串
   * @param compressionCodecName Name to identify the codec used to compress the contents
   *                             of the log, or None if compression is not enabled.
    *                             用于标识用于压缩日志内容的编解码器的名称,如果未启用压缩，则为“无”
   * @return A path which consists of file-system-safe characters.由文件系统安全字符组成的路径
   */
  def getLogPath(
      logBaseDir: URI,
      appId: String,
      appAttemptId: Option[String],
      compressionCodecName: Option[String] = None): String = {
    //stripSuffix去掉<string>字串中结尾的字符
    val base = logBaseDir.toString.stripSuffix("/") + "/" + sanitize(appId)
    val codec = compressionCodecName.map("." + _).getOrElse("")
    if (appAttemptId.isDefined) {
      base + "_" + sanitize(appAttemptId.get) + codec
    } else {
      base + codec
    }
  }

  private def sanitize(str: String): String = {
    str.replaceAll("[ :/]", "-").replaceAll("[.${}'\"]", "_").toLowerCase
  }

  /**
   * Opens an event log file and returns an input stream that contains the event data.
   * 打开一个事件日志文件,并返回包含事件数据的输入流
   * @return input stream that holds one JSON record per line.
   */
  def openEventLog(log: Path, fs: FileSystem): InputStream = {
    // It's not clear whether FileSystem.open() throws FileNotFoundException or just plain
    // IOException when a file does not exist, so try our best to throw a proper exception.
    //FileSystem.open（）是否抛出FileNotFoundException或者是纯文本还不清楚
    //IOException当一个文件不存在时，所以尽量扔一个正确的异常。
    if (!fs.exists(log)) {
      throw new FileNotFoundException(s"File $log does not exist.")
    }

    val in = new BufferedInputStream(fs.open(log))

    // Compression codec is encoded as an extension, e.g. app_123.lzf
    // Since we sanitize the app ID to not include periods, it is safe to split on it
    //压缩编解码器被编码为扩展, app_123.lzf由于我们将应用ID清理为不包含句点,因此可以安全地拆分它
    val logName = log.getName.stripSuffix(IN_PROGRESS)
    val codecName: Option[String] = logName.split("\\.").tail.lastOption
    val codec = codecName.map { c =>
      codecMap.getOrElseUpdate(c, CompressionCodec.createCodec(new SparkConf, c))
    }

    try {
      codec.map(_.compressedInputStream(in)).getOrElse(in)
    } catch {
      case e: Exception =>
        in.close()
        throw e
    }
  }

}
