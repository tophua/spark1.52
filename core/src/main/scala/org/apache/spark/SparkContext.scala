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

import scala.language.implicitConversions

import java.io._
import java.lang.reflect.Constructor
import java.net.URI
import java.util.{Arrays, Properties, UUID}
import java.util.concurrent.atomic.{AtomicReference, AtomicBoolean, AtomicInteger}
import java.util.UUID.randomUUID

import scala.collection.{Map, Set}
import scala.collection.JavaConversions._
import scala.collection.generic.Growable
import scala.collection.mutable.HashMap
import scala.reflect.{ClassTag, classTag}
import scala.util.control.NonFatal

import org.apache.commons.lang.SerializationUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{ArrayWritable, BooleanWritable, BytesWritable, DoubleWritable,
  FloatWritable, IntWritable, LongWritable, NullWritable, Text, Writable}
import org.apache.hadoop.mapred.{FileInputFormat, InputFormat, JobConf, SequenceFileInputFormat,
  TextInputFormat}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat, Job => NewHadoopJob}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => NewFileInputFormat}

import org.apache.mesos.MesosNativeLibrary

import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.{LocalSparkCluster, SparkHadoopUtil}
import org.apache.spark.executor.{ExecutorEndpoint, TriggerThreadDump}
import org.apache.spark.input.{StreamInputFormat, PortableDataStream, WholeTextFileInputFormat,
  FixedLengthBinaryInputFormat}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.partial.{ApproximateEvaluator, PartialResult}
import org.apache.spark.rdd._
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend,
  SparkDeploySchedulerBackend, SimrSchedulerBackend}
import org.apache.spark.scheduler.cluster.mesos.{CoarseMesosSchedulerBackend, MesosSchedulerBackend}
import org.apache.spark.scheduler.local.LocalBackend
import org.apache.spark.storage._
import org.apache.spark.ui.{SparkUI, ConsoleProgressBar}
import org.apache.spark.ui.jobs.JobProgressListener
import org.apache.spark.util._

/**
 * Main entry point for Spark functionality. A SparkContext represents the connection to a Spark
 * Spark功能的主要入口点,一个SparkContext代表连接Spark集群
 * cluster, and can be used to create RDDs, accumulators and broadcast variables on that cluster.
 * 可以用来创建RDDS,在集群可以创建累加器和广播变量
 * Only one SparkContext may be active per JVM.  You must `stop()` the active SparkContext before
 * 每个JVM只有一个sparkcontext可能激活,创建一个新的SparkContext之前必须'stop()'.这种限制最终可能会被删除
 * creating a new one.  This limitation may eventually be removed; see SPARK-2243 for more details.
 *
 * SparkContext的初始化步骤如下:
 * 1)创建Spark执行环境SparkEnv
 * 2)创建RDD清理器metadataCleaner
 * 3)创建并初始化SparkUI
 * 4)Hadoop相关配置及Executor环境变量的设置
 * 5)创建任务调度TaskScheduler
 * 6)创建和启动DAGScheduler
 * 7)TashScheduler的启动
 * 8)初始化块管理器BlockManager(BlockManager是存储体系的主要组件之一)
 * 9)启动测量系统MetricsSystem
 * 10)创建和启动Executor分配 管理器ExecutorAllocationManager
 * 11)ContextCleaner的创建与启动
 * 12)Spark环境更新
 * 13)创建DAGSchedulerSource和BlockManagerSource
 * 14)将SparkContext标记为激活
 * @param config a Spark Config object describing the application configuration. Any settings in
 *   this config overrides the default configs as well as system properties.
 *   
 */
class SparkContext(config: SparkConf) extends Logging with ExecutorAllocationClient {

  // The call site where this SparkContext was constructed.
  //存储了线程栈中最靠近栈顶的用户类及最近栈底的scala或者Spark核心信息
  private val creationSite: CallSite = Utils.getCallSite()

  // If true, log warnings instead of throwing exceptions when multiple SparkContexts are active
  // 如果为true,多个激活sparkcontexts日志警告,而不是抛出异常
  //SparkContext默认只有一个实例
  private val allowMultipleContexts: Boolean =
    config.getBoolean("spark.driver.allowMultipleContexts", false)

  // In order to prevent multiple SparkContexts from being active at the same time, mark this 
  // context as having started construction.
  //为了防止同时激活多个sparkcontexts,标记当前上下文正在构建中
  // NOTE: this must be placed at the beginning of the SparkContext constructor.
  //注意:这必须放在sparkcontext构造函数的开始
  //用来确保实例的唯一性,并将当前Spark标记为正在构建中
  SparkContext.markPartiallyConstructed(this, allowMultipleContexts)

  // This is used only by YARN for now, but should be relevant to other cluster types (Mesos,  
  // etc) too. This is typically generated from InputFormatInfo.computePreferredLocations.
  //现在只用于的YARN,但也可以是其他相关的类型集群,这通常来自computePreferredLocations(计算的首选地点)
  // It contains a map from hostname to a list of input format splits on the host.
  //Map的Key主机的输入格式,value
  //它包含主机名的列表
  private[spark] var preferredNodeLocationData: Map[String, Set[SplitInfo]] = Map()
  //系统开始运行时间
  val startTime = System.currentTimeMillis()
  //暂停标示
  private[spark] val stopped: AtomicBoolean = new AtomicBoolean(false)
  //如果已经停止抛出异常
  private def assertNotStopped(): Unit = {
    if (stopped.get()) {
      throw new IllegalStateException("Cannot call methods on a stopped SparkContext")
    }
  }

  /**
   * Create a SparkContext that loads settings from system properties (for instance, when  
   * launching with ./bin/spark-submit).
   * 创建一个sparkcontext加载设置系统属性(例如,当启动./bin/spark-submit)
   */
  def this() = this(new SparkConf())

  /**
   * :: DeveloperApi ::
   * Alternative constructor for setting preferred locations where Spark will create executors.
   * 用于设置Spark将创建执行程序的首选位置的替代构造函数
   * @param config a [[org.apache.spark.SparkConf]] object specifying other Spark parameters
   * 					    指定其他Spark参数的对象
   * @param preferredNodeLocationData used in YARN mode to select nodes to launch containers on.
   * 																	用于在YARN模式选择节点来启动容器	
   * Can be generated using [[org.apache.spark.scheduler.InputFormatInfo.computePreferredLocations]]
   * from a list of input files or InputFormats for the application.
    * 可以从应用程序的输入文件或InputFormat列表中使用
    * [[org.apache.spark.scheduler.InputFormatInfo.computePreferredLocations]]生成
   */
  @deprecated("Passing in preferred locations has no effect at all, see SPARK-8949", "1.5.0")
  @DeveloperApi
  def this(config: SparkConf, preferredNodeLocationData: Map[String, Set[SplitInfo]]) = {
    this(config)
    logWarning("Passing in preferred locations has no effect at all, see SPARK-8949")
    this.preferredNodeLocationData = preferredNodeLocationData
  }

  /**
   * Alternative constructor that allows setting common Spark properties directly
   * 允许直接设置公共Spark属性的替代构造函数
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI
   * 							你的应用程序的名称,在群集Web用户界面上显示
   * @param conf a [[org.apache.spark.SparkConf]] object specifying other Spark parameters
   * 				指定其他Spark参数的对象
   */
  def this(master: String, appName: String, conf: SparkConf) =
    this(SparkContext.updatedConf(conf, master, appName))

  /**
   * Alternative constructor that allows setting common Spark properties directly
   * 允许直接设置公共Spark属性的替代构造函数
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI.
   * 				         你的应用程序的名称,在群集Web用户界面上显示
   * @param sparkHome Location where Spark is installed on cluster nodes.
   * 				          Spark安装在群集节点上的位置
   * @param jars Collection of JARs to send to the cluster. These can be paths on the local file 			
   *             system or HDFS, HTTP, HTTPS, or FTP URLs.
    *        jar的集合发送到集群,这些可以是本地文件系统或HDFS,HTTP,HTTPS或FTP URL的路径
   * @param environment Environment variables to set on worker nodes.
   * 				            在工作节点上设置的环境变量
   * @param preferredNodeLocationData used in YARN mode to select nodes to launch containers on.
   * 				                           用于在YARN模式选择节点来启动容器
   * Can be generated using [[org.apache.spark.scheduler.InputFormatInfo.computePreferredLocations]]
   * from a list of input files or InputFormats for the application.
    *
    * 可以从应用程序的输入文件或InputFormat列表中使用[[org.apache.spark.scheduler.InputFormatInfo.computePreferredLocations]]生成
   */
  def this(
      master: String,
      appName: String,
      sparkHome: String = null,
      jars: Seq[String] = Nil,//列表结尾为Nil
      environment: Map[String, String] = Map(),
      preferredNodeLocationData: Map[String, Set[SplitInfo]] = Map()) =
  {
    this(SparkContext.updatedConf(new SparkConf(), master, appName, sparkHome, jars, environment))
    //nonEmpty 非空
    if (preferredNodeLocationData.nonEmpty) {
      //在优先位置传递完全没有影响
      logWarning("Passing in preferred locations has no effect at all, see SPARK-8949")
    }
    this.preferredNodeLocationData = preferredNodeLocationData
  }

  // NOTE: The below constructors could be consolidated using default arguments. Due to
  // 注意：下面的构造函数可以使用默认参数合并,但是,这会导致编译步骤在生成文档时失败,直到我们有一个很好的解决方法为该bug,构造函数仍然被破坏。
  // Scala bug SI-8479, however, this causes the compile step to fail when generating docs.
  // Until we have a good workaround for that bug the constructors remain broken out.

  /**
   * Alternative constructor that allows setting common Spark properties directly
 	 * 允许直接设置公共Spark属性的替代构造函数
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI.
    *                应用程序的名称,以在集群Web UI上显示
   */
  private[spark] def this(master: String, appName: String) =
    this(master, appName, null, Nil, Map(), Map())//列表结尾为Nil

  /**
   * Alternative constructor that allows setting common Spark properties directly
   * 允许直接设置公共Spark属性的替代构造函数
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI.
   * @param sparkHome Location where Spark is installed on cluster nodes.Spark安装在群集节点上的位置
   */
  private[spark] def this(master: String, appName: String, sparkHome: String) =
    this(master, appName, sparkHome, Nil, Map(), Map())//列表结尾为Nil

  /**
   * Alternative constructor that allows setting common Spark properties directly
   * 允许直接设置公共Spark属性的替代构造函数
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI.
   * @param sparkHome Location where Spark is installed on cluster nodes.Spark安装在群集节点上的位置
   * @param jars Collection of JARs to send to the cluster. These can be paths on the local file
   *             system or HDFS, HTTP, HTTPS, or FTP URLs.
    *             jar的集合发送到集群,这些可以是本地文件系统或HDFS,HTTP,HTTPS或FTP URL的路径
   */
  private[spark] def this(master: String, appName: String, sparkHome: String, jars: Seq[String]) =
    this(master, appName, sparkHome, jars, Map(), Map())

  // log out Spark Version in Spark driver log
  // 在Spark驱动程序日志中记录Spark版本
  logInfo(s"Running Spark version $SPARK_VERSION")

  /* ------------------------------------------------------------------------------------- *
   | Private variables. These variables keep the internal state of the context, and are    |
   | 私有变量.这些变量保持上下文的内部状态,并不可访问的外部
   | not accessible by the outside world. They're mutable since we want to initialize all  |
   | of them to some neutral value ahead of time, so that calling "stop()" while the       |
   | 他们可变的的需要提前初始化中性值,所以调用“stop()”当构造函数运行是安全的。
   | constructor is still running is safe.                                                 |
   * ------------------------------------------------------------------------------------- */

  private var _conf: SparkConf = _
  private var _eventLogDir: Option[URI] = None
  private var _eventLogCodec: Option[String] = None
  private var _env: SparkEnv = _
  private var _metadataCleaner: MetadataCleaner = _
  private var _jobProgressListener: JobProgressListener = _
  private var _statusTracker: SparkStatusTracker = _
  private var _progressBar: Option[ConsoleProgressBar] = None
  private var _ui: Option[SparkUI] = None
  private var _hadoopConfiguration: Configuration = _
  private var _executorMemory: Int = _
  private var _schedulerBackend: SchedulerBackend = _
  private var _taskScheduler: TaskScheduler = _
  private var _heartbeatReceiver: RpcEndpointRef = _
  @volatile private var _dagScheduler: DAGScheduler = _
  private var _applicationId: String = _
  private var _applicationAttemptId: Option[String] = None
  private var _eventLogger: Option[EventLoggingListener] = None
  private var _executorAllocationManager: Option[ExecutorAllocationManager] = None
  private var _cleaner: Option[ContextCleaner] = None
  private var _listenerBusStarted: Boolean = false
  private var _jars: Seq[String] = _
  private var _files: Seq[String] = _
  private var _shutdownHookRef: AnyRef = _

  /* ------------------------------------------------------------------------------------- *
   | Accessors and public fields. These provide access to the internal state of the        |
   | context.                                                                              |
   | 访问公有的字段,这些提供访问上下文的内部状态
   * ------------------------------------------------------------------------------------- */

  private[spark] def conf: SparkConf = _conf

  /**
   * Return a copy of this SparkContext's configuration. The configuration ''cannot'' be
   * changed at runtime.
   * 对SparkConf的配置文件进行复制,这个配置文件属性运行时不可改变.
   */
  def getConf: SparkConf = conf.clone()

  def jars: Seq[String] = _jars
  def files: Seq[String] = _files
  //要连接的Spark集群Master的URL
  def master: String = _conf.get("spark.master")
  //应用程序名称
  def appName: String = _conf.get("spark.app.name")
 //是否记录Spark事件
  private[spark] def isEventLogEnabled: Boolean = _conf.getBoolean("spark.eventLog.enabled", false)
  private[spark] def eventLogDir: Option[URI] = _eventLogDir
  private[spark] def eventLogCodec: Option[String] = _eventLogCodec

  // Generate the random name for a temp folder in external block store.
  // Add a timestamp as the suffix here to make it more safe
  //为外部块存储中的临时文件夹生成随机名称,添加时间戳作为后缀,使其更安全
  val externalBlockStoreFolderName = "spark-" + randomUUID.toString()
  @deprecated("Use externalBlockStoreFolderName instead.", "1.4.0")
  val tachyonFolderName = externalBlockStoreFolderName
  //是否单机模式
  def isLocal: Boolean = (master == "local" || master.startsWith("local["))

  // An asynchronous listener bus for Spark events,Spark事件异步监听总线
  //listenerBus采用异步监听器模式维护各类事件的处理
  //LiveListenerBus实现了监听器模型,通过监听事件触发对各种监听状态信息的修改,达到UI界面的数据刷新效果
  private[spark] val listenerBus = new LiveListenerBus

  // This function allows components created by SparkEnv to be mocked in unit tests:
  //这个功能允许创建的sparkenv,在单元测试中被模拟
  //SparkEnv是一个很重要的变量,其内包括了很多Spark执行时的重要组件（变量）,包括 MapOutputTracker、ShuffleFetcher、BlockManager等,
  // 这里是通过SparkEnv类的伴生对象SparkEnv Object内的Create方法实现的
  private[spark] def createSparkEnv(
      conf: SparkConf,
      isLocal: Boolean,
      listenerBus: LiveListenerBus): SparkEnv = {
    SparkEnv.createDriverEnv(conf, isLocal, listenerBus, SparkContext.numDriverCores(master))
  }

  private[spark] def env: SparkEnv = _env

  // Used to store a URL for each static file/jar together with the file's local timestamp
  //用于将每个静态文件/ jar的URL与文件的本地时间戳一起存储
  private[spark] val addedFiles = HashMap[String, Long]() 
  private[spark] val addedJars = HashMap[String, Long]()

  // Keeps track of all persisted RDDs
  //保持对所有持久化的RDD跟踪,使用TimeStampedWeakValueHashMap的persistentRdds缓存
  private[spark] val persistentRdds = new TimeStampedWeakValueHashMap[Int, RDD[_]]
  //清除过期的持久化RDD
  private[spark] def metadataCleaner: MetadataCleaner = _metadataCleaner
  //任务进度
  private[spark] def jobProgressListener: JobProgressListener = _jobProgressListener
  //Spark状态跟踪
  def statusTracker: SparkStatusTracker = _statusTracker
  //进度条
  private[spark] def progressBar: Option[ConsoleProgressBar] = _progressBar

  private[spark] def ui: Option[SparkUI] = _ui

  /**
   * A default Hadoop Configuration for the Hadoop code (e.g. file systems) that we reuse.
   * 我们重用的Hadoop代码(例如文件系统)的默认Hadoop配置
   * '''Note:''' As it will be reused in all Hadoop RDDs, it's better not to modify it unless you
   * plan to set some global configurations for all Hadoop RDDs.
    * 由于它将在所有Hadoop RDD中重复使用,最好不要修改它,除非您计划为所有Hadoop RDD设置一些全局配置
   */
  def hadoopConfiguration: Configuration = _hadoopConfiguration

  private[spark] def executorMemory: Int = _executorMemory

  // Environment variables to pass to our executors.
  //环境变量传递给executors
  private[spark] val executorEnvs = HashMap[String, String]()

  // Set SPARK_USER for user who is running SparkContext.
  // 返回当前用户名,这是当前登录的用户
  val sparkUser = Utils.getCurrentUserName()

  private[spark] def schedulerBackend: SchedulerBackend = _schedulerBackend
  private[spark] def schedulerBackend_=(sb: SchedulerBackend): Unit = {
    _schedulerBackend = sb
  }

  private[spark] def taskScheduler: TaskScheduler = _taskScheduler
  private[spark] def taskScheduler_=(ts: TaskScheduler): Unit = {
    _taskScheduler = ts
  }

  private[spark] def dagScheduler: DAGScheduler = _dagScheduler
  private[spark] def dagScheduler_=(ds: DAGScheduler): Unit = {
    _dagScheduler = ds
  }

  /**
   * A unique identifier for the Spark application.
   * Its format depends on the scheduler implementation.
   * Spark应用程序唯一标示,它的格式取决于调度程序的实现
   * (i.e.
   *  in case of local spark app something like 'local-1433865536131'
   *  在Spark应用的本地模式情况下,类似的东西'local-1433865536131'
   *  in case of YARN something like 'application_1433865536131_34483'
   *  在Spark应用的YARN模式情况下,类似的东西'application_1433865536131_34483'
   * )
   */
  def applicationId: String = _applicationId
  def applicationAttemptId: Option[String] = _applicationAttemptId

  def metricsSystem: MetricsSystem = if (_env != null) _env.metricsSystem else null

  private[spark] def eventLogger: Option[EventLoggingListener] = _eventLogger

  private[spark] def executorAllocationManager: Option[ExecutorAllocationManager] =
    _executorAllocationManager

  private[spark] def cleaner: Option[ContextCleaner] = _cleaner

  private[spark] var checkpointDir: Option[String] = None

  // Thread Local variable that can be used by users to pass information down the stack
  // 线程本地变量,这可以通过用户传递信息到堆栈
  protected[spark] val localProperties = new InheritableThreadLocal[Properties] {
    override protected def childValue(parent: Properties): Properties = {
      // Note: make a clone such that changes in the parent properties aren't reflected in
      // the those of the children threads, which has confusing semantics (SPARK-10563).
      //注意：使一个克隆,使得父属性中的更改不会反映在具有混淆语义(SPARK-10563)的子线程中
      if (conf.get("spark.localProperties.clone", "false").toBoolean) {
        SerializationUtils.clone(parent).asInstanceOf[Properties]
      } else {
        new Properties(parent)
      }
    }
    override protected def initialValue(): Properties = new Properties()
  }

  /* ------------------------------------------------------------------------------------- *
   | Initialization. This code initializes the context in a manner that is exception-safe. |
   | All internal fields holding state are initialized here, and any error prompts the     |
   | 初始化,此代码初始化上下文中的方式是异常安全,所有保持状态的内部字段都在这里初始化,                    |
   | stop() method to be called.任务错误提示将被调用stop方法                                    |
   * ------------------------------------------------------------------------------------- */

  private def warnSparkMem(value: String): String = {
    logWarning("Using SPARK_MEM to set amount of memory to use per executor process is " +
      "deprecated, please use spark.executor.memory instead.")
    value
  }

  /** 
   * Control our logLevel. This overrides any user-defined log settings.
   * 控制日志级别,这将覆盖任何自定义日志设置
   * @param logLevel The desired log level as a string. 所需的日志级别作为一个字符串
   * Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
   * 有效日志级别包括:ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
   */
  def setLogLevel(logLevel: String) {
    val validLevels = Seq("ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN")
    if (!validLevels.contains(logLevel)) {
      throw new IllegalArgumentException(
      s"Supplied level $logLevel did not match one of: ${validLevels.mkString(",")}")
    }
    Utils.setLogLevel(org.apache.log4j.Level.toLevel(logLevel))
  }
  //初始化代码块
  try {
    //对SparkCon进行复制
    _conf = config.clone()
    //对Spark各种信息进行校验
    _conf.validateSettings()
    //必须指定spark.master,spark.app.name属性,否是会抛出异常,结束初始化过程
    if (!_conf.contains("spark.master")) {
      throw new SparkException("A master URL must be set in your configuration")
    }
    if (!_conf.contains("spark.app.name")) {
      throw new SparkException("An application name must be set in your configuration")
    }

    // System property spark.yarn.app.id must be set if user code ran by AM on a YARN cluster
    // yarn-standalone is deprecated, but still supported
    // 系统属性spark.yarn.app.id必须设置,如果用户代码跑的是YARN独立集群,已经过时,但一直支持
    if ((master == "yarn-cluster" || master == "yarn-standalone") &&
        !_conf.contains("spark.yarn.app.id")) {
      throw new SparkException("Detected yarn-cluster mode, but isn't running on a cluster. " +
        "Deployment to YARN is not supported directly by SparkContext. Please use spark-submit.")
    }
    //SparkContext 启动时是否记录有效 SparkConf信息
    if (_conf.getBoolean("spark.logConf", false)) {
      logInfo("Spark configuration:\n" + _conf.toDebugString)
    }

    // Set Spark driver host and port system properties
    //设置Sparkr的driver主机名或 IP地址和端口属性
    _conf.setIfMissing("spark.driver.host", Utils.localHostName())
    //0随机 driver侦听的端口
    _conf.setIfMissing("spark.driver.port", "0")
    //设置executor.id为driver
    _conf.set("spark.executor.id", SparkContext.DRIVER_IDENTIFIER)

    _jars = _conf.getOption("spark.jars").map(_.split(",")).map(_.filter(_.size != 0)).toSeq.flatten//转换 
    _files = _conf.getOption("spark.files").map(_.split(",")).map(_.filter(_.size != 0)).toSeq.flatten
   //保存日志相关信息的路径,可以是hdfs://开头的HDFS路径,也可以是file://开头的本地路径,都需要提前创建
    _eventLogDir =
      if (isEventLogEnabled) {
        val unresolvedDir = conf.get("spark.eventLog.dir", EventLoggingListener.DEFAULT_LOG_DIR)
          //stripSuffix去掉<string>字串中结尾的字符
          .stripSuffix("/")
        Some(Utils.resolveURI(unresolvedDir))
      } else {
        None
      }

    _eventLogCodec = {
    //是否压缩记录Spark事件,前提spark.eventLog.enabled为true
      val compress = _conf.getBoolean("spark.eventLog.compress", false)
      if (compress && isEventLogEnabled) {
        Some(CompressionCodec.getCodecName(_conf)).map(CompressionCodec.getShortName)
      } else {
        None
      }
    }

    _conf.set("spark.externalBlockStore.folderName", externalBlockStoreFolderName)

    if (master == "yarn-client") System.setProperty("SPARK_YARN_MODE", "true")

    // "_jobProgressListener" should be set up before creating SparkEnv because when creating
    //"_jobProgressListener"应用在创建SparkEnv之前,需要创建JobProgressListener
    // "SparkEnv", some messages will be posted to "listenerBus" and we should not miss them.
    //"SparkEnv",一些消息将被提交到"listenerBus"我们不应该错过他们
    //构造JobProgressListener,作用是通过HashMap,ListBuffer等数据结构存储JobId及对应JobUIData信息,并按照激活
    //完成,失败等job状态统计,对于StageId,StageInfo等信息按照激活,完成,忽略,失败等Stage状态统计,并且存储StageID
    //与JobId的一对多关系.
    _jobProgressListener = new JobProgressListener(_conf)//通过监听listenerBus中的事件更新任务进度
    //添加事件
    listenerBus.addListener(jobProgressListener)

    // Create the Spark execution environment (cache, map output tracker, etc)
    //创建Spark执行环境(缓存, 任务输出跟踪,等等)
    _env = createSparkEnv(_conf, isLocal, listenerBus)
    SparkEnv.set(_env)
    //清除过期的持久化RDD,构造MetadataCleaner时的参数是cleanup,用于清理persistentRdds
    _metadataCleaner = new MetadataCleaner(MetadataCleanerType.SPARK_CONTEXT, this.cleanup, _conf)

    _statusTracker = new SparkStatusTracker(this)

    _progressBar =
      if (_conf.getBoolean("spark.ui.showConsoleProgress", true) && !log.isInfoEnabled) {
        Some(new ConsoleProgressBar(this))
      } else {
        None
      }

    _ui =
      if (conf.getBoolean("spark.ui.enabled", true)) {
        Some(SparkUI.createLiveUI(this, _conf, listenerBus, _jobProgressListener,
          _env.securityManager, appName, startTime = startTime))
      } else {
        // For tests, do not enable the UI对于测试,不要启用用户界面
        None
      }
    // Bind the UI before starting the task scheduler to communicate
    //在启动任务计划程序之前绑定该用户界面
    // the bound port to the cluster manager properly
    //绑定端口到群集管理器
    _ui.foreach(_.bind())
   //默认情况下:Spark使用HDFS作为分布式文件系统,所以需要获取Hadoop相关配置信息
    _hadoopConfiguration = SparkHadoopUtil.get.newConfiguration(_conf)

    // Add each JAR given through the constructor
    //通过构造函数添加每个jar
    if (jars != null) {
      jars.foreach(addJar)
    }

    if (files != null) {
      files.foreach(addFile)
    }
    //spark.executor.memory指定分配给每个executor进程总内存,也可以配置系统变量SPARK_EXECUTOR_MEMORY对其大小进行设置
    //Master给Worker发送高度后,worker最终使用executorEnvs提供的信息启动Executor
    _executorMemory = _conf.getOption("spark.executor.memory")//分配给每个executor进程总内存
      .orElse(Option(System.getenv("SPARK_EXECUTOR_MEMORY")))
      .orElse(Option(System.getenv("SPARK_MEM"))
      .map(warnSparkMem))//获得Spark
      .map(Utils.memoryStringToMb)
      .getOrElse(1024)//默认值 1024

    // Convert java options to env vars as a work around
    //转换java选项到evn变量作为一个工作节点
    // since we can't set env vars directly in sbt.
    //既然我们不能设置环境变量直接在SBT
     //Executor环境变量executorEnvs
    for { (envKey, propKey) <- Seq(("SPARK_TESTING", "spark.testing"))
      value <- Option(System.getenv(envKey)).orElse(Option(System.getProperty(propKey)))} {
      executorEnvs(envKey) = value
    }
    Option(System.getenv("SPARK_PREPEND_CLASSES")).foreach { v =>
      executorEnvs("SPARK_PREPEND_CLASSES") = v
    }
    // The Mesos scheduler backend relies on this environment variable to set executor memory.
    //使用Mesos调度器的后端,此环境变量设置执行器的内存
    // TODO: Set this only in the Mesos scheduler.
    executorEnvs("SPARK_EXECUTOR_MEMORY") = executorMemory + "m"
    executorEnvs ++= _conf.getExecutorEnv
    executorEnvs("SPARK_USER") = sparkUser //设置Spark_user 用户名

    // We need to register "HeartbeatReceiver" before "createTaskScheduler" because Executor will
    //在"createTaskScheduler"之前,我们需要注册"HeartbeatReceiver", 因为执行器将接收“heartbeatreceiver”的构造函数
    // retrieve "HeartbeatReceiver" in the constructor. (SPARK-6640)
    //注册一个HeartbeatReceiver消息
    _heartbeatReceiver = env.rpcEnv.setupEndpoint(
      HeartbeatReceiver.ENDPOINT_NAME, new HeartbeatReceiver(this))

    // Create and start the scheduler
    //创建任务调度器:负责任务的提交,并且请求集群管理器对任务调度,TaskSchedule可以看做任务调度的客户端
    //createTaskScheduler方法会根据master的配置匹配部署模式,创建TaskSchedulerImpl,并生成不同的SchedulerBanckend
    val (sched, ts) = SparkContext.createTaskScheduler(this, master)
    _schedulerBackend = sched
    _taskScheduler = ts
    //DAGScheduler主要用于在任务正式交给TaskSchedulerImpl提交之前做一些准备工作
    //包括创建Job,将DAG中的RDD划分到不同的Stage,提交Stage等
    
    _dagScheduler = new DAGScheduler(this)
    // 主要目的 scheduler = sc.taskScheduler
    //HeartbeatReceiver接收TaskSchedulerIsSet消息,设置scheduler = sc.taskScheduler
    //启动Start方法,向自己发送ExpireDeadHosts,检测Executor心跳
    _heartbeatReceiver.ask[Boolean](TaskSchedulerIsSet)

    // start TaskScheduler after taskScheduler sets DAGScheduler reference in DAGScheduler's
    // constructor
    _taskScheduler.start() //启动任务调度器

    _applicationId = _taskScheduler.applicationId()//获得应用程序ID
    _applicationAttemptId = taskScheduler.applicationAttemptId()
    _conf.set("spark.app.id", _applicationId)
    _env.blockManager.initialize(_applicationId)

    // The metrics system for Driver need to be set spark.app.id to app ID.
    // 驱动器测量系统需要设置spark.app.id
    // So it should start after we get app ID from the task scheduler and set spark.app.id.    
    metricsSystem.start()//启动测量系统
    // Attach the driver metrics servlet handler to the web ui after the metrics system is started.
    //Web UI的度量系统启动后,驱动测量servlet处理
    metricsSystem.getServletHandlers.foreach(handler => ui.foreach(_.attachHandler(handler)))

    _eventLogger =
      if (isEventLogEnabled) {
        val logger =
          new EventLoggingListener(_applicationId, _applicationAttemptId, _eventLogDir.get,
            _conf, _hadoopConfiguration)
        logger.start()
        listenerBus.addListener(logger)
        Some(logger)
      } else {
        None
      }

    // Optionally scale number of executors dynamically based on workload. Exposed for testing.
    //动态分配最小Executor数量,动态分配最大Executor数量,每个Executor可以运行的Task的数量等配置信息
    val dynamicAllocationEnabled = Utils.isDynamicAllocationEnabled(_conf)
    if (!dynamicAllocationEnabled && _conf.getBoolean("spark.dynamicAllocation.enabled", false)) {
      logInfo("Dynamic Allocation and num executors both set, thus dynamic allocation disabled.")
    }
    //对已分配的Executor进行管理,创建和启动ExecutorAllocationManager
    _executorAllocationManager =
      if (dynamicAllocationEnabled) {
        //动态分配最小Executor数量,动态分配最大Executor数量,每个Executor可以运行的Task的数量等配置信息
        Some(new ExecutorAllocationManager(this, listenerBus, _conf))
      } else {
        None
      }
    //启动动态分配
    _executorAllocationManager.foreach(_.start())
    //ContextCleaner用于清理超出应用范围的RDD、ShuffleDependency和Broadcast对象
    //默认为true
    _cleaner =
      if (_conf.getBoolean("spark.cleaner.referenceTracking", true)) {
        Some(new ContextCleaner(this))
      } else {
        None
      }
    _cleaner.foreach(_.start())

    setupAndStartListenerBus()
    //更新Spark环境
    postEnvironmentUpdate()
    postApplicationStart()

    // Post init
    _taskScheduler.postStartHook()
    //注册测量信息
    _env.metricsSystem.registerSource(new BlockManagerSource(_env.blockManager))
    _executorAllocationManager.foreach { e =>
      _env.metricsSystem.registerSource(e.executorAllocationManagerSource)
    }

    // Make sure the context is stopped if the user forgets about it. This avoids leaving
    //确保上下文被停止,如果用户忘记它,这样可以避免留下未完成的事件日志在JVM退出后清理
    // unfinished event logs around after the JVM exits cleanly. It doesn't help if the JVM
    // is killed, though.
    // 如果JVM被杀死,它不帮助
    _shutdownHookRef = ShutdownHookManager.addShutdownHook(
      ShutdownHookManager.SPARK_CONTEXT_SHUTDOWN_PRIORITY) { () =>
      logInfo("Invoking stop() from shutdown hook")
      stop()
    }
  } catch {
    case NonFatal(e) =>
      logError("Error initializing SparkContext.", e)
      try {
        stop()
      } catch {
        case NonFatal(inner) =>
          logError("Error stopping SparkContext after init error.", inner)
      } finally {
        throw e
      }
  }

  /**
   * Called by the web UI to obtain executor thread dumps.  This method may be expensive.
   * 由Web用户界面调用,获取执行线程转储,这种方法可能是耗时的
   * Logs an error and returns None if we failed to obtain a thread dump, which could occur due
   * 记录一个错误,并返回没有,如果我们没有获得一个线程转储,
   * to an executor being dead or unresponsive or due to network issues while sending the thread
   * 这可能会发生一个执行器已经死了或没有响应或由于网络问题,同时发送线程转储消息返回到驱动程序
   * dump message back to the driver.  
   */
  private[spark] def getExecutorThreadDump(executorId: String): Option[Array[ThreadStackTrace]] = {
    try {
      if (executorId == SparkContext.DRIVER_IDENTIFIER) {
        Some(Utils.getThreadDump())
      } else {
        val (host, port) = env.blockManager.master.getRpcHostPortForExecutor(executorId).get
        val endpointRef = env.rpcEnv.setupEndpointRef(
          SparkEnv.executorActorSystemName,
          RpcAddress(host, port),
          ExecutorEndpoint.EXECUTOR_ENDPOINT_NAME)
        Some(endpointRef.askWithRetry[Array[ThreadStackTrace]](TriggerThreadDump))
      }
    } catch {
      case e: Exception =>
        logError(s"Exception getting thread dump from executor $executorId", e)
        None
    }
  }

  private[spark] def getLocalProperties: Properties = localProperties.get()

  private[spark] def setLocalProperties(props: Properties) {
    localProperties.set(props)
  }

  @deprecated("Properties no longer need to be explicitly initialized.", "1.0.0")
  def initLocalProperties() {
    localProperties.set(new Properties())
  }

  /**
   * Set a local property that affects jobs submitted from this thread, such as the
   * Spark fair scheduler pool.   
   * 设置一个本地属性影响提交作业Job的线程,这是Spark公平调度池
   */
  def setLocalProperty(key: String, value: String) {
    if (value == null) {
      localProperties.get.remove(key)
    } else {
      localProperties.get.setProperty(key, value)
    }
  }

  /**
   * Get a local property set in this thread, or null if it is missing. See
   * [[org.apache.spark.SparkContext.setLocalProperty]].
   * 获取此线程中的本地属性,如果null,则缺失
   */
  def getLocalProperty(key: String): String =
    //如果非空,则返回该选项的值,如果为空则返回“null”。
    Option(localProperties.get).map(_.getProperty(key)).orNull

  /** 
   *  Set a human readable description of the current job. 
   *  设置当前作业可读的描述
   *  */
  def setJobDescription(value: String) {
    setLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION, value)
  }

  /**
   * Assigns a group ID to all the jobs started by this thread until the group ID is set to a
   * different value or cleared.
   * 将一组标识分配给该线程所启动的所有作业,直到将该组标识设置为不同的值或清除
   * 
   * Often, a unit of execution in an application consists of multiple Spark actions or jobs.
   * 通常,在应用程序中的一个执行器单元由多个Spark动作或作业组成
   * Application programmers can use this method to group all those jobs together and give a
   * group description. Once set, the Spark web UI will associate such jobs with this group.
   * 应用程序程序可以使用这种方法将所有工作组在一起并给出一组描述,一旦设置,Spark用户界面将把这类工作与这组
   * 
   * The application can also use [[org.apache.spark.SparkContext.cancelJobGroup]] to cancel all
   * running jobs in this group. For example,取消本组所有的作业,例子
   * {{{
   * // In the main thread: 在主线程中：
   * sc.setJobGroup("some_job_to_cancel", "some job description")
   * sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(10); i }.count()
   *
   * // In a separate thread: 在一个单独的线程中
   * sc.cancelJobGroup("some_job_to_cancel")
   * }}}
   *
   * If interruptOnCancel is set to true for the job group, then job cancellation will result
   * 如果interruptoncancel设置为true的工作组,在一个线程Thread.interrupt()取消作业的结果
   * in Thread.interrupt() being called on the job's executor threads. This is useful to help ensure
   * 被调用该作业的执行线程,这是有用的,确保任务实际上是及时停止的
   * that the tasks are actually stopped in a timely manner, but is off by default due to HDFS-1208,
   * where HDFS may respond to Thread.interrupt() by marking nodes as dead.
   */
  def setJobGroup(groupId: String, description: String, interruptOnCancel: Boolean = false) {
    setLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION, description)
    setLocalProperty(SparkContext.SPARK_JOB_GROUP_ID, groupId)
    // Note: Specifying interruptOnCancel in setJobGroup (rather than cancelJobGroup) avoids
    // changing several public APIs and allows Spark cancellations outside of the cancelJobGroup
    // APIs to also take advantage of this property (e.g., internal job failures or canceling from
    // JobProgressTab UI) on a per-job basis.
    setLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, interruptOnCancel.toString)
  }

  /** 
   *  Clear the current thread's job group ID and its description. 
   *  清除当前线程的Job组标识的描述
   *  */
  def clearJobGroup() {
    setLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION, null)
    setLocalProperty(SparkContext.SPARK_JOB_GROUP_ID, null)
    setLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, null)
  }

  /**
   * Execute a block of code in a scope such that all new RDDs created in this body will
   * be part of the same scope. For more detail, see {{org.apache.spark.rdd.RDDOperationScope}}.
   * 在范围内执行一个代码块,创建所有新的RDDS将相同的范围
   * 
   * Note: Return statements are NOT allowed in the given body.
   * 注:在给定的代码体的所有内容中不允许返回语句
   */
  private[spark] def withScope[U](body: => U): U = RDDOperationScope.withScope[U](this)(body)

  // Methods for creating RDDs

  /** 
   *  Distribute a local Scala collection to form an RDD.
   *  分配一个本地Seq的集合创建一个RDD
   * @note Parallelize acts lazily. If `seq` is a mutable collection and is altered after the call
   * 延迟并发执行,如果'SEQ'是一个可变的集合和改变后调用并行化,
   * to parallelize and before the first action on the RDD, the resultant RDD will reflect the
   * 在RDD第一个执行之前,所得的RDD将反回修改后集合,通过一个参数的副本,以避免此
   * modified collection. Pass a copy of the argument to avoid this.
   * @note avoid using `parallelize(Seq())` to create an empty `RDD`. Consider `emptyRDD` for an
   * RDD with no partitions, or `parallelize(Seq[T]())` for an RDD of `T` with empty partitions.
    *
    * 避免使用`parallelize（Seq（））`来创建一个空的`RDD`,
    * 考虑一下`emptyRDD`RDD没有分区,或者`parallelize（Seq [T]（））`用于具有空分区的“T”的RDD。
    *
    * ClassTag 用于编译器在运行时也能获取泛型类型的信息,在JVM上,泛型参数类型T在运行时是被“擦拭”掉的,编译器把T当作Object来对待,
    * 所以T的具体信息是无法得到的,为了使得在运行时得到T的信息,
   */
  def parallelize[T: ClassTag](
      seq: Seq[T],
      numSlices: Int = defaultParallelism): RDD[T] = withScope {
    assertNotStopped()//判断是否暂停
    //并发集体RDD
    new ParallelCollectionRDD[T](this, seq, numSlices, Map[Int, Seq[String]]())
  }

  /**
   * Creates a new RDD[Long] containing elements from `start` to `end`(exclusive), increased by
   * `step` every element.
   * 创建一个新RDD,包含元素开始和结束,每个元素的增量
   * @note if we need to cache this RDD, we should make sure each partition does not exceed limit.
   * 如果我们需要缓存该RDD,我们应该确保每个分区不超过限制
   *
   * @param start the start value.开始值
   * @param end the end value. 结束值
   * @param step the incremental step 每个元素的增量步骤
   * @param numSlices the partition number of the new RDD.新RDD的分区数
   * @return
   */
  def range(
      start: Long,
      end: Long,
      step: Long = 1,
      numSlices: Int = defaultParallelism): RDD[Long] = withScope {
    assertNotStopped()
    // when step is 0, range will run infinitely
    //当步骤为0时,范围将无限运行
    require(step != 0, "step cannot be 0")
    val numElements: BigInt = {
      val safeStart = BigInt(start)
      val safeEnd = BigInt(end)
      if ((safeEnd - safeStart) % step == 0 || safeEnd > safeStart ^ step > 0) {
        (safeEnd - safeStart) / step
      } else {
        // the remainder has the same sign with range, could add 1 more
        //其余的有相同的符号与范围,可以添加1个以上
        (safeEnd - safeStart) / step + 1
      }
    }
    parallelize(0 until numSlices, numSlices).mapPartitionsWithIndex((i, _) => {
      val partitionStart = (i * numElements) / numSlices * step + start
      val partitionEnd = (((i + 1) * numElements) / numSlices) * step + start
      def getSafeMargin(bi: BigInt): Long =
        if (bi.isValidLong) {
          bi.toLong
        } else if (bi > 0) {
          Long.MaxValue
        } else {
          Long.MinValue
        }
      val safePartitionStart = getSafeMargin(partitionStart)
      val safePartitionEnd = getSafeMargin(partitionEnd)

      new Iterator[Long] {
        private[this] var number: Long = safePartitionStart
        private[this] var overflow: Boolean = false

        override def hasNext =
          if (!overflow) {
            if (step > 0) {
              number < safePartitionEnd
            } else {
              number > safePartitionEnd
            }
          } else false

        override def next() = {
          val ret = number
          number += step
          if (number < ret ^ step < 0) {
            // we have Long.MaxValue + Long.MaxValue < Long.MaxValue
            // and Long.MinValue + Long.MinValue > Long.MinValue, so iff the step causes a step
            // back, we are pretty sure that we have an overflow.
            overflow = true
          }
          ret
        }
      }
    })
  }

  /** Distribute a local Scala collection to form an RDD.
   * 分配一个本地Seq的集合创建一个RDD  
   * This method is identical to `parallelize`.
   * 这种方法是`并行`相同
   */
  def makeRDD[T: ClassTag](
      seq: Seq[T],
      numSlices: Int = defaultParallelism): RDD[T] = withScope {
    parallelize(seq, numSlices)
  }

  /** 
   *  Distribute a local Scala collection to form an RDD, with one or more
   *  分配一个本地Seq的集合创建一个RDD,
    * location preferences (hostnames of Spark nodes) for each object.
    * 每个一个或者是多个位置偏好对象(Spark节点主机名)
    * Create a new partition for each collection item.
    * 为每个集合项目创建一个新分区
    *  */
  def makeRDD[T: ClassTag](seq: Seq[(T, Seq[String])]): RDD[T] = withScope {
    assertNotStopped()
    val indexToPrefs = seq.zipWithIndex.map(t => (t._2, t._1._2)).toMap
    new ParallelCollectionRDD[T](this, seq.map(_._1), seq.size, indexToPrefs)
  }

  /**
   * Read a text file from HDFS, a local file system (available on all nodes), or any
   * Hadoop-supported file system URI, and return it as an RDD of Strings.
   * 从HDFS读取文本文件,本地文件系统(可在所有节点上),或者Hadoop支持的URI文件系统
   * 返回一个RDD的字符串
   */
  def textFile(
      path: String,
      minPartitions: Int = defaultMinPartitions): RDD[String] = withScope {
    assertNotStopped()
    //调用hadoopFile方法,生成MappedRDD对象
    hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
     //map方法将MappedRDD封装为MapPartitionsRDD
      minPartitions).map(pair => pair._2.toString)
  }

  /**
   * Read a directory of text files from HDFS, a local file system (available on all nodes), or any
   * Hadoop-supported file system URI. Each file is read as a single record and returned in a
   * key-value pair, where the key is the path of each file, the value is the content of each file.
   * 在HDFS中读一个目录下的文本文件,本地文件系统(可在所有节点上),Hadoop文件系统或任何支持URI,
   * 每个文件被读取为一个记录,并返回一个关键值对,其中键是每个文件的路径,值是每个文件的内容
   * <p> For example, if you have the following files:
   * {{{
   *   hdfs://a-hdfs-path/part-00000
   *   hdfs://a-hdfs-path/part-00001
   *   ...
   *   hdfs://a-hdfs-path/part-nnnnn
   * }}}
   *
   * Do `val rdd = sparkContext.wholeTextFile("hdfs://a-hdfs-path")`,
   *
   * <p> then `rdd` contains
   * {{{
   *   (a-hdfs-path/part-00000, its content)
   *   (a-hdfs-path/part-00001, its content)
   *   ...
   *   (a-hdfs-path/part-nnnnn, its content)
   * }}}
   *
   * @note Small files are preferred, large file is also allowable, but may cause bad performance.
   * 			 小文件优先考虑,大文件也是允许的,但可能会造成不好的表现
   * @note On some filesystems, `.../path/&#42;` can be a more efficient way to read all files
   * 			 可以是一个更有效的方式来读取目录中的所有文件
   *       in a directory rather than `.../path/` or `.../path`
   *
   * @param path Directory to the input data files, the path can be comma separated paths as the
   * 				输入数据的目录文件,可以是以逗号分隔的输入列表的路径
   *             list of inputs.
   * @param minPartitions A suggestion value of the minimal splitting number for input data.
   * 				最小分区数,输入数据的最小拆分数建议值
   */
  def wholeTextFiles(
      path: String,
      minPartitions: Int = defaultMinPartitions): RDD[(String, String)] = withScope {
    assertNotStopped()
    val job = new NewHadoopJob(hadoopConfiguration)
    // Use setInputPaths so that wholeTextFiles aligns with hadoopFile/textFile in taking
    // comma separated files as input. (see SPARK-7155)
    //使用setInputPaths,以使整数文件与hadoopFile/textFile对齐,以逗号分隔的文件作为输入,(见SPARK-7155)
    //FileInputFormat 任务划分
    NewFileInputFormat.setInputPaths(job, path)
    val updateConf = job.getConfiguration
    new WholeTextFileRDD(
      this,
      classOf[WholeTextFileInputFormat],
      classOf[String],
      classOf[String],
      updateConf,
      minPartitions).setName(path)
  }

  /**
   * :: Experimental ::
   *
   * Get an RDD for a Hadoop-readable dataset as PortableDataStream for each file
   * (useful for binary data)
   * 在HDFS中读一个目录下的二进行文件,每个文件被读取为一个记录,
   * 并返回一个关键值对,其中键是每个文件的路径,值是每个文件的内容
   * For example, if you have the following files:
   * {{{
   *   hdfs://a-hdfs-path/part-00000
   *   hdfs://a-hdfs-path/part-00001
   *   ...
   *   hdfs://a-hdfs-path/part-nnnnn
   * }}}
   *
   * Do
   * `val rdd = sparkContext.binaryFiles("hdfs://a-hdfs-path")`,
   *
   * then `rdd` contains
   * {{{
   *   (a-hdfs-path/part-00000, its content)
   *   (a-hdfs-path/part-00001, its content)
   *   ...
   *   (a-hdfs-path/part-nnnnn, its content)
   * }}}
   *
   * @note Small files are preferred; very large files may cause bad performance.
    *      小文件是首选的,非常大的文件可能会导致坏的性能
   * @note On some filesystems, `.../path/&#42;` can be a more efficient way to read all files
   *       in a directory rather than `.../path/` or `.../path`
   *
   * @param path Directory to the input data files, the path can be comma separated paths as the
   *             list of inputs.将目录输入到数据文件中,路径可以以逗号分隔的路径作为输入的列表。
   * @param minPartitions A suggestion value of the minimal splitting number for input data.
    *                      输入数据最小分裂数的一个建议值
   */
  @Experimental
  def binaryFiles(
      path: String,
      minPartitions: Int = defaultMinPartitions): RDD[(String, PortableDataStream)] = withScope {
    assertNotStopped()
    val job = new NewHadoopJob(hadoopConfiguration)
    // Use setInputPaths so that binaryFiles aligns with hadoopFile/textFile in taking
    // comma separated files as input. (see SPARK-7155)
    //使用setinputpaths这样binaryfiles与hadoopfile/文本文件以逗号分隔的文件作为输入
    NewFileInputFormat.setInputPaths(job, path)
    val updateConf = job.getConfiguration
    new BinaryFileRDD(
      this,
      classOf[StreamInputFormat],
      classOf[String],
      classOf[PortableDataStream],
      updateConf,
      minPartitions).setName(path)
  }

  /**
   * :: Experimental ::
   *
   * Load data from a flat binary file, assuming the length of each record is constant.
   * 加载二进制数据文件,假设每个记录的长度是不变的
   * '''Note:''' We ensure that the byte array for each record in the resulting RDD
   * 我们确保在产生的RDD每条记录的字节数组,具有提供的记录长度
   * has the provided record length.
   *
   * @param path Directory to the input data files, the path can be comma separated paths as the   
   *             list of inputs.
   *     		输入数据文件的路径目录,路径可以是以逗号分隔的输入列表
   * @param recordLength The length at which to split the records 拆分记录的长度
   * @param conf Configuration for setting up the dataset. 设置数据集的配置
   *
   * @return An RDD of data with values, represented as byte arrays
   * 				数据值的RDD,表示为字节数组
   */
  @Experimental
  def binaryRecords(
      path: String,
      recordLength: Int,
      conf: Configuration = hadoopConfiguration): RDD[Array[Byte]] = withScope {
    assertNotStopped()
    conf.setInt(FixedLengthBinaryInputFormat.RECORD_LENGTH_PROPERTY, recordLength)
    val br = newAPIHadoopFile[LongWritable, BytesWritable, FixedLengthBinaryInputFormat](path,
      classOf[FixedLengthBinaryInputFormat],
      classOf[LongWritable],
      classOf[BytesWritable],
      conf = conf)
    val data = br.map { case (k, v) =>
      val bytes = v.getBytes
      assert(bytes.length == recordLength, "Byte array does not have correct length")
      bytes
    }
    data
  }

  /**
   * Get an RDD for a Hadoop-readable dataset from a Hadoop JobConf given its InputFormat and other
   * 从 Hadoop可读的数据集获得一个 Hadoop jobconf的RDD,
   * necessary info (e.g. file name for a filesystem-based dataset, table name for HyperTable),
   * (例如,文件名为基于文件系统的数据集,对Hypertable表名)
   * using the older MapReduce API (`org.apache.hadoop.mapred`).使用旧的MapReduce API
   *
   * @param conf JobConf for setting up the dataset. Note: This will be put into a Broadcast.
   * 		设立jobconf的数据集,注:这将被放入一个广播变量,
   *             Therefore if you plan to reuse this conf to create multiple RDDs, you need to make
   *             因此,如果你打算使用这个配置创建多个RDDS,你需要确保你不会修改conf
   *             sure you won't modify the conf. A safe approach is always creating a new conf for
   *             a new RDD.一个安全的方法是创建一个新的RDD配置
   * @param inputFormatClass Class of the InputFormat
   * @param keyClass Class of the keys
   * @param valueClass Class of the values
   * @param minPartitions Minimum number of Hadoop Splits to generate.Hadoop分隔产生的最低分区数量
   *
   * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
   * 为每个记录由于Hadoop的RecordReader类重新使用相同可写的对象
   * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
   * operation will create many references to the same object.
   * 直接返回缓存的RDD或直接传递到聚集或Shuufle操作会产生很多引用相同的对象
   * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
   * 如果你计划直接缓存,排序,或Hadoop聚合可写的对象,你应该首先将它们复制使用`Map`功能
   * copy them using a `map` function.
   */
  def hadoopRDD[K, V](
      conf: JobConf,
      inputFormatClass: Class[_ <: InputFormat[K, V]],
      keyClass: Class[K],
      valueClass: Class[V],
      minPartitions: Int = defaultMinPartitions): RDD[(K, V)] = withScope {
    assertNotStopped()
    // Add necessary security credentials to the JobConf before broadcasting it.
    //广播前添加必要的安全认证到jobconf
    SparkHadoopUtil.get.addCredentials(conf)
    new HadoopRDD(this, conf, inputFormatClass, keyClass, valueClass, minPartitions)
  }

  /** Get an RDD for a Hadoop file with an arbitrary InputFormat  
   *  任意一个InputFormat获得一个Hadoop文件的RDD,
   * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
   * operation will create many references to the same object.
    * 注：'因为Hadoop的RecordReader类重新使用相同的可写的对象为每个记录,直接缓存返回的RDD或直接传递到聚集或洗牌操作会产生很多引用相同的对象
   * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
   * copy them using a `map` function.
    *如果您计划直接缓存排序或聚合Hadoop可写对象,应该首先使用“map”函数复制它们
   */
  def hadoopFile[K, V](
      path: String,
      inputFormatClass: Class[_ <: InputFormat[K, V]],
      keyClass: Class[K],
      valueClass: Class[V],
      minPartitions: Int = defaultMinPartitions): RDD[(K, V)] = withScope {
    assertNotStopped()
    // A Hadoop configuration can be about 10 KB, which is pretty big, so broadcast it.
    //Hadoop的配置约10 kb,这是相当大的,所以广播
    //将Hadoop的Configuration封装为SerializableWritable用于序列化读写操作,然后广播Hadoop的Configuration
    val confBroadcast = broadcast(new SerializableConfiguration(hadoopConfiguration))
    //定义偏函数,设置输入路径
    val setInputPathsFunc = (jobConf: JobConf) => FileInputFormat.setInputPaths(jobConf, path)
    //构建HadoopRDD
    new HadoopRDD(
      this,//this代表SparkContext
      confBroadcast,
      Some(setInputPathsFunc),
      inputFormatClass,
      keyClass,
      valueClass,
      minPartitions).setName(path)
  }

  /**
   * Smarter version of hadoopFile() that uses class tags to figure out the classes of keys,
   * 优化版本hadoopfile()使用类标签来找出关键的类
   * values and the InputFormat so that users don't need to pass them directly. Instead, callers
   * 值和InputFormat,让用户不需要直接通过.
   * can just write, for example,相反,调用方可以只写,例如
   * {{{
   * val file = sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](path, minPartitions)
   * }}}
   *
   * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
   * operation will create many references to the same object.
   * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
   * copy them using a `map` function.
    * 注：因为Hadoop的RecordReader类重新使用相同的可写的对象为每个记录,
    * 直接缓存返回的RDD或直接传递到聚集或洗牌操作会产生很多引用相同的对象。
    * 如果您计划直接缓存,排序或聚合Hadoop可写对象,应该首先使用“map”函数复制它们。
   */
  def hadoopFile[K, V, F <: InputFormat[K, V]]
      (path: String, minPartitions: Int)
      (implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F]): RDD[(K, V)] = withScope {
    hadoopFile(path,
      fm.runtimeClass.asInstanceOf[Class[F]],
      km.runtimeClass.asInstanceOf[Class[K]],
      vm.runtimeClass.asInstanceOf[Class[V]],
      minPartitions)
  }

  /**
   * Smarter version of hadoopFile() that uses class tags to figure out the classes of keys,
   * values and the InputFormat so that users don't need to pass them directly. Instead, callers
   * can just write, for example,
   * {{{
   * val file = sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](path)
   * }}}
   *
   * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
   * operation will create many references to the same object.
   * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
   * copy them using a `map` function.
   */
  def hadoopFile[K, V, F <: InputFormat[K, V]](path: String)
      (implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F]): RDD[(K, V)] = withScope {
    hadoopFile[K, V, F](path, defaultMinPartitions)
  }

  /** 
   *  Get an RDD for a Hadoop file with an arbitrary new API InputFormat.
   *    任意一个新的API InputFormat获得一个Hadoop文件的RDD,
   *  */
  def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]]
      (path: String)
      (implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F]): RDD[(K, V)] = withScope {
    newAPIHadoopFile(
      path,
      fm.runtimeClass.asInstanceOf[Class[F]],
      km.runtimeClass.asInstanceOf[Class[K]],
      vm.runtimeClass.asInstanceOf[Class[V]])
  }

  /**
   * Get an RDD for a given Hadoop file with an arbitrary new API InputFormat
   *  任意一个新的API InputFormat获得一个Hadoop文件的RDD,和额外的配置选项传递给输入格式
   * and extra configuration options to pass to the input format.
   *
   * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
   * operation will create many references to the same object.
   * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
   * copy them using a `map` function.
   */
  def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]](
      path: String,
      fClass: Class[F],
      kClass: Class[K],
      vClass: Class[V],
      conf: Configuration = hadoopConfiguration): RDD[(K, V)] = withScope {
    assertNotStopped()
    // The call to new NewHadoopJob automatically adds security credentials to conf,
    // so we don't need to explicitly add them ourselves
    //调用新的newhadoopjob自动添加安全凭据来配置，所以我们不需要显式地添加他们自己
    val job = new NewHadoopJob(conf)
    // Use setInputPaths so that newAPIHadoopFile aligns with hadoopFile/textFile in taking
    // comma separated files as input. (see SPARK-7155)
    NewFileInputFormat.setInputPaths(job, path)
    val updatedConf = job.getConfiguration
    new NewHadoopRDD(this, fClass, kClass, vClass, updatedConf).setName(path)
  }

  /**
   * Get an RDD for a given Hadoop file with an arbitrary new API InputFormat
   * and extra configuration options to pass to the input format.
    * 在给定的任意新的API InputFormat和额外的配置选项传递给输入格式Hadoop文件RDD
   *
   * @param conf Configuration for setting up the dataset. Note: This will be put into a Broadcast.
   *             Therefore if you plan to reuse this conf to create multiple RDDs, you need to make
   *             sure you won't modify the conf. A safe approach is always creating a new conf for
   *             a new RDD.
    *             设置数据源的配置,注意：这将被放入一个广播,因此,如果你打算使用这个配置创建多个RDDs,
    *             你需要确保你不会修改设置一个安全的方法是创建一个新的RDD新配置。
   * @param fClass Class of the InputFormat
   * @param kClass Class of the keys
   * @param vClass Class of the values
   *
   * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
   * operation will create many references to the same object.
   * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
   * copy them using a `map` function.
   */
  def newAPIHadoopRDD[K, V, F <: NewInputFormat[K, V]](
      conf: Configuration = hadoopConfiguration,
      fClass: Class[F],
      kClass: Class[K],
      vClass: Class[V]): RDD[(K, V)] = withScope {
    assertNotStopped()
    // Add necessary security credentials to the JobConf. Required to access secure HDFS.
    //增加必要的安全凭据的jobconf,需要安全访问HDFS
    val jconf = new JobConf(conf)
    SparkHadoopUtil.get.addCredentials(jconf)
    new NewHadoopRDD(this, fClass, kClass, vClass, jconf)
  }

  /** Get an RDD for a Hadoop SequenceFile with given key and value types.
    * 得到一个RDD为Hadoop SequenceFile与给定的键和值的类型
    * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
    * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
    * operation will create many references to the same object.
    * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
    * copy them using a `map` function.
    */
  def sequenceFile[K, V](path: String,
      keyClass: Class[K],
      valueClass: Class[V],
      minPartitions: Int
      ): RDD[(K, V)] = withScope {
    assertNotStopped()
    val inputFormatClass = classOf[SequenceFileInputFormat[K, V]]
    hadoopFile(path, inputFormatClass, keyClass, valueClass, minPartitions)
  }

  /** Get an RDD for a Hadoop SequenceFile with given key and value types.
    * 给定的键和值的类型得到一个RDD为Hadoop SequenceFile。
    * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
    * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
    * operation will create many references to the same object.
    * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
    * copy them using a `map` function.
    * */
  def sequenceFile[K, V](
      path: String,
      keyClass: Class[K],
      valueClass: Class[V]): RDD[(K, V)] = withScope {
    assertNotStopped()
    sequenceFile(path, keyClass, valueClass, defaultMinPartitions)
  }

  /**
   * Version of sequenceFile() for types implicitly convertible to Writables through a
   * WritableConverter. For example, to access a SequenceFile where the keys are Text and the
   * values are IntWritable, you could simply write
    * 对于隐式转换为writables通过类型sequencefile()版WritableConverter,例如,访问通过按键位置的文本和值intwritable
   * {{{
   * sparkContext.sequenceFile[String, Int](path, ...)
   * }}}
   *
   * WritableConverters are provided in a somewhat strange way (by an implicit function) to support
   * both subclasses of Writable and types for which we define a converter (e.g. Int to
   * IntWritable). The most natural thing would've been to have implicit objects for the
   * converters, but then we couldn't have an object for every subclass of Writable (you can't
   * have a parameterized singleton object). We use functions instead to create a new converter
   * for the appropriate type. In addition, we pass the converter a ClassTag of its type to
   * allow it to figure out the Writable class to use in the subclass case.
    *
    * writableconverters是在一个有点奇怪的方式提供（由隐函数）来支持写和类型子类,我们定义了一个转换器(例如int intwritable),
    * 最自然的事情是对转换器有隐式对象,但是对于每个可写的子类,我们不能有一个对象(不能有参数化的单例对象),
    * 我们使用函数来创建一个新的合适的类型转换器,此外,我们通过转换器的classtag允许它出用在子类的情况下写的类类型。
   *
   * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
   * operation will create many references to the same object.
   * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
   * copy them using a `map` function.
   */
   def sequenceFile[K, V]
       (path: String, minPartitions: Int = defaultMinPartitions)
       (implicit km: ClassTag[K], vm: ClassTag[V],
        kcf: () => WritableConverter[K], vcf: () => WritableConverter[V]): RDD[(K, V)] = {
    withScope {
      assertNotStopped()
      val kc = clean(kcf)()
      val vc = clean(vcf)()
      val format = classOf[SequenceFileInputFormat[Writable, Writable]]
      val writables = hadoopFile(path, format,
        kc.writableClass(km).asInstanceOf[Class[Writable]],
        vc.writableClass(vm).asInstanceOf[Class[Writable]], minPartitions)
      writables.map { case (k, v) => (kc.convert(k), vc.convert(v)) }
    }
  }

  /**
   * Load an RDD saved as a SequenceFile containing serialized objects, with NullWritable keys and
   * 加载一个RDD保存为SequenceFile包含序列化的对象,与nullwritable键和byteswritable值包含序列化的分区
   * BytesWritable values that contain a serialized partition. This is still an experimental
   * storage format and may not be supported exactly as is in future Spark releases. It will also
   * 这仍然是一个实验性的存储格式,可能不支持正是在未来的Spark发布
   * be pretty slow if you use the default serializer (Java serialization),
   * 如果您使用默认的序列化程序很慢
   * though the nice thing about it is that there's very little effort required to save arbitrary
   * objects.
   * 虽然很好的事情是,有很少的努力,需要保存任意对象
   */
  def objectFile[T: ClassTag](
      path: String,
      minPartitions: Int = defaultMinPartitions): RDD[T] = withScope {
    assertNotStopped()
    sequenceFile(path, classOf[NullWritable], classOf[BytesWritable], minPartitions)
      .flatMap(x => Utils.deserialize[Array[T]](x._2.getBytes, Utils.getContextOrSparkClassLoader))
  }

  protected[spark] def checkpointFile[T: ClassTag](path: String): RDD[T] = withScope {
    new ReliableCheckpointRDD[T](this, path)
  }

  /** 
   *  Build the union of a list of RDDs. 
   *  构建合并RDDS的一个列表
   *  */
  def union[T: ClassTag](rdds: Seq[RDD[T]]): RDD[T] = withScope {
    val partitioners = rdds.flatMap(_.partitioner).toSet
    if (rdds.forall(_.partitioner.isDefined) && partitioners.size == 1) {
      new PartitionerAwareUnionRDD(this, rdds)
    } else {
      new UnionRDD(this, rdds)
    }
  }

  /** 
   *  Build the union of a list of RDDs passed as variable-length arguments.
   *  构建合并RDDS的一个列表,可变参数长度
   *   */
  def union[T: ClassTag](first: RDD[T], rest: RDD[T]*): RDD[T] = withScope {
    union(Seq(first) ++ rest)
  }

  /** 
   *  Get an RDD that has no partitions or elements.
   *  获得一个RDD没有分区或元素
   *   */
  def emptyRDD[T: ClassTag]: EmptyRDD[T] = new EmptyRDD[T](this)

  // Methods for creating shared variables
  // 创建共享变量的方法

  /**
   * Create an [[org.apache.spark.Accumulator]] variable of a given type, which tasks can "add"
   * values to using the `+=` method. Only the driver can access the accumulator's `value`.
   * 给定一个类型创建累加器变量,任务可以使用+=增加值,只有驱动器可以访问累加器变量的值
   */
  def accumulator[T](initialValue: T)(implicit param: AccumulatorParam[T]): Accumulator[T] =
  {
    val acc = new Accumulator(initialValue, param)
    cleaner.foreach(_.registerAccumulatorForCleanup(acc))
    acc
  }

  /**
   * Create an [[org.apache.spark.Accumulator]] variable of a given type, with a name for display
   * in the Spark UI. Tasks can "add" values to the accumulator using the `+=` method. Only the
   * 给定一个类型创建累加器变量,在Spark用户界面中显示的名称,任务可以使用+=增加值,只有驱动器可以访问累加器变量的值
   * driver can access the accumulator's `value`.
   */
  def accumulator[T](initialValue: T, name: String)(implicit param: AccumulatorParam[T])
    : Accumulator[T] = {
    val acc = new Accumulator(initialValue, param, Some(name))
    cleaner.foreach(_.registerAccumulatorForCleanup(acc))
    acc
  }

  /**
   * Create an [[org.apache.spark.Accumulable]] shared variable, to which tasks can add values
   * with `+=`. Only the driver can access the accumuable's `value`.
   * 给定一个类型创建累加器共享变量,任务可以使用+=增加值,只有驱动器可以访问累加器变量的值
   * @tparam R accumulator result type,累加器结果类型
   * @tparam T type that can be added to the accumulator 可以添加到累加器的类型
   */
  def accumulable[R, T](initialValue: R)(implicit param: AccumulableParam[R, T])
    : Accumulable[R, T] = {
    val acc = new Accumulable(initialValue, param)
    cleaner.foreach(_.registerAccumulatorForCleanup(acc))
    acc
  }

  /**
   * Create an [[org.apache.spark.Accumulable]] shared variable, with a name for display in the
   * Spark UI. Tasks can add values to the accumuable using the `+=` operator. Only the driver can
   * access the accumuable's `value`.
   * 给定一个类型创建累加器共享变量,在Spark用户界面中显示的名称,任务可以使用+=增加值,只有驱动器可以访问累加器变量的值
   * @tparam R accumulator result type 累加器结果类型
   * @tparam T type that can be added to the accumulator 可以添加到累加器的类型
   */
  def accumulable[R, T](initialValue: R, name: String)(implicit param: AccumulableParam[R, T])
    : Accumulable[R, T] = {
    val acc = new Accumulable(initialValue, param, Some(name))
    cleaner.foreach(_.registerAccumulatorForCleanup(acc))
    acc
  }

  /**
   * Create an accumulator from a "mutable collection" type.
   * 创建一个累加器来自一个可变的集合类
   * Growable and TraversableOnce are the standard APIs that guarantee += and ++=, implemented by
   * 增长性和traversableonce是标准的API保证+= and ++=,
   * standard mutable collections. So you can use this with mutable Map, Set, etc.
   * 通过实施标准可变集合,所以你可以使用这个易变的Map,set等
   */
  def accumulableCollection[R <% Growable[T] with TraversableOnce[T] with Serializable: ClassTag, T]
      (initialValue: R): Accumulable[R, T] = {
    val param = new GrowableAccumulableParam[R, T]
    val acc = new Accumulable(initialValue, param)
    cleaner.foreach(_.registerAccumulatorForCleanup(acc))
    acc
  }

  /**
   * Broadcast a read-only variable to the cluster, returning a
   * [[org.apache.spark.broadcast.Broadcast]] object for reading it in distributed functions.
   * 广播一个只读变量到群集,返回一个Broadcast对象,在分布式功能中读取,
   * The variable will be sent to each cluster only once.
   * 该变量只有一次将被发送到每个群集
   */
  def broadcast[T: ClassTag](value: T): Broadcast[T] = {
    assertNotStopped()
    if (classOf[RDD[_]].isAssignableFrom(classTag[T].runtimeClass)) {
      // This is a warning instead of an exception in order to avoid breaking user programs that
      //这是一个警告而不是异常为了避免用户程序可能已经创建了RDD广播变量而不使用它们
      // might have created RDD broadcast variables but not used them:
      //可能已经创建了RDD广播变量而不使用它们:
      logWarning("Can not directly broadcast RDDs; instead, call collect() and "
        + "broadcast the result (see SPARK-5063)")
    }
    val bc = env.broadcastManager.newBroadcast[T](value, isLocal)
    val callSite = getCallSite
    logInfo("Created broadcast " + bc.id + " from " + callSite.shortForm)
    //广播结束将广播对象注册到ContextCleanner中,以便清理.
    cleaner.foreach(_.registerBroadcastForCleanup(bc))
    bc
  }

  /**
   * Add a file to be downloaded with this Spark job on every node.
   * 添加一个文件在每个Spark作业节点下载使用
   * The `path` passed can be either a local file, a file in HDFS (or other Hadoop-supported
   * 它的路径可以本地文件或者HDFS中的文件(或其他Hadoop的支持系统文件）or an HTTP, HTTPS or FTP URI
   * filesystems), or an HTTP, HTTPS or FTP URI.  To access the file in Spark jobs,
   * 访问Spark作业中的文件,使用'sparkfiles.get(fileName)'找到下载位置
   * use `SparkFiles.get(fileName)` to find its download location.
   */
  def addFile(path: String): Unit = {
    addFile(path, false)
  }

  /**
   * Add a file to be downloaded with this Spark job on every node.
   * 添加一个文件在每个Spark作业节点下载使用
   * The `path` passed can be either a local file, a file in HDFS (or other Hadoop-supported
   * filesystems), or an HTTP, HTTPS or FTP URI.  To access the file in Spark jobs,
   * 它的路径可以本地文件或者HDFS中的文件(或其他Hadoop的支持系统文件）or an HTTP, HTTPS or FTP URI
   * use `SparkFiles.get(fileName)` to find its download location.
   * 访问Spark作业中的文件,使用'sparkfiles.get(fileName)'找到下载位置
   * 
   * A directory can be given if the recursive option is set to true. Currently directories are only
   * supported for Hadoop-supported filesystems.
   * 如果recursive选项设置为真,则可以给出一个目录,目前目录只支持Hadoop支持的文件系统
   */
  def addFile(path: String, recursive: Boolean): Unit = {
    val uri = new URI(path)
    val schemeCorrectedPath = uri.getScheme match {
        //D:\workspace\test\..\src\test1.txt getAbsolutePath 相对路径，返回当前目录的路径
        //D:\workspace\src\test1.txt getCanonicalFile 不但是全路径,而且把..或者.这样的符号解析出来
      case null | "local" => new File(path).getCanonicalFile.toURI.toString
      case _ => path
    }

    val hadoopPath = new Path(schemeCorrectedPath)
    val scheme = new URI(schemeCorrectedPath).getScheme
    if (!Array("http", "https", "ftp").contains(scheme)) {
      val fs = hadoopPath.getFileSystem(hadoopConfiguration)
      if (!fs.exists(hadoopPath)) {
        throw new FileNotFoundException(s"Added file $hadoopPath does not exist.")
      }
      val isDir = fs.getFileStatus(hadoopPath).isDir
      if (!isLocal && scheme == "file" && isDir) {
        throw new SparkException(s"addFile does not support local directories when not running " +
          "local mode.")
      }
      if (!recursive && isDir) {
        throw new SparkException(s"Added file $hadoopPath is a directory and recursive is not " +
          "turned on.")
      }
    }

    val key = if (!isLocal && scheme == "file") {
      env.httpFileServer.addFile(new File(uri.getPath))
    } else {
      schemeCorrectedPath
    }

    val timestamp = System.currentTimeMillis
    addedFiles(key) = timestamp

    // Fetch the file locally in case a job is executed using DAGScheduler.runLocally().
    //在一个作业执行runLocally时获取本地文件
    Utils.fetchFile(path, new File(SparkFiles.getRootDirectory()), conf, env.securityManager,
      hadoopConfiguration, timestamp, useCache = false)

    logInfo("Added file " + path + " at " + key + " with timestamp " + addedFiles(key))
    postEnvironmentUpdate()
  }

  /**
   * :: DeveloperApi ::
   * Register a listener to receive up-calls from events that happen during execution.
   * 注册一个侦听器,接收在执行过程中发生事件的调用
   */
  @DeveloperApi
  def addSparkListener(listener: SparkListener) {
    listenerBus.addListener(listener)
  }

  /**
   * Update the cluster manager on our scheduling needs. Three bits of information are included
   * to help it make decisions.
   * 根据我们的调度需求更新集群管理器,三位参数信息被包括来帮助它作出决定
   * @param numExecutors The total number of executors we'd like to have. The cluster manager
   *                     shouldn't kill any running executor to reach this number, but,
   *                     我们需要总执行器数,集群管理器不应该杀死任何运行的执行器,以达到这个数字
   *                     if all existing executors were to die, this is the number of executors
   *                     we'd want to be allocated.
   *                     如果存在的执行器已死了,我们重新分配这些执行器
   * @param localityAwareTasks The number of tasks in all active stages that have a locality
   *                           preferences. This includes running, pending, and completed tasks.
   *                           一个最佳位置的所有活动阶段的任务数,这包括运行、挂起和完成任务
   * @param hostToLocalTaskCount A map of hosts to the number of tasks from all active stages
   *                             that would like to like to run on that host.
   *                             一个Map的Key主机,value所有活动阶段的任务数量,就喜欢在那个主机上运行
   *                             This includes running, pending, and completed tasks.
   *                             包括运行、挂起和完成任务
   *                             
   * @return whether the request is acknowledged by the cluster manager.
   */
  private[spark] override def requestTotalExecutors(
      numExecutors: Int,
      localityAwareTasks: Int,
      hostToLocalTaskCount: scala.collection.immutable.Map[String, Int]
    ): Boolean = {
    schedulerBackend match {
      case b: CoarseGrainedSchedulerBackend =>
        b.requestTotalExecutors(numExecutors, localityAwareTasks, hostToLocalTaskCount)
      case _ =>
        logWarning("Requesting executors is only supported in coarse-grained mode")
        false
    }
  }

  /**
   * :: DeveloperApi ::
   * Request an additional number of executors from the cluster manager.
   * 从群集管理器请求添加执行器数
   * @return whether the request is received.是否收到请求
   */
  @DeveloperApi
  override def requestExecutors(numAdditionalExecutors: Int): Boolean = {
    schedulerBackend match {
      case b: CoarseGrainedSchedulerBackend =>
        b.requestExecutors(numAdditionalExecutors)
      case _ =>
        logWarning("Requesting executors is only supported in coarse-grained mode")
        false
    }
  }

  /**
   * :: DeveloperApi ::
   * Request that the cluster manager kill the specified executors.
   * 请求集群管理器杀死执行器的列表
   * Note: This is an indication to the cluster manager that the application wishes to adjust
   * its resource usage downwards. If the application wishes to replace the executors it kills
   * through this method with new ones, it should follow up explicitly with a call to
   * {{SparkContext#requestExecutors}}.
    * 注意：这表明应用程序希望向下调整其资源使用情况,如果应用程序想要替换执行器,
    * 它会通过这种方法杀死新的执行程序,它应该通过调用明确跟踪requestExecutors
   *
   * @return whether the request is received. 是否收到请求
   */
  @DeveloperApi
  override def killExecutors(executorIds: Seq[String]): Boolean = {
    schedulerBackend match {
      case b: CoarseGrainedSchedulerBackend =>
        b.killExecutors(executorIds)
      case _ =>
        logWarning("Killing executors is only supported in coarse-grained mode")
        false
    }
  }

  /**
   * :: DeveloperApi ::
   * Request that the cluster manager kill the specified executor.
   * 请求集群管理器杀死指定的执行器
   * Note: This is an indication to the cluster manager that the application wishes to adjust
   * its resource usage downwards. If the application wishes to replace the executor it kills
   * through this method with a new one, it should follow up explicitly with a call to
   * {{SparkContext#requestExecutors}}.
    * 注意：这表明应用程序希望向下调整其资源使用情况,如果应用程序想要替换执行器
    * 它会通过这种方法杀死新的执行程序,它应该通过调用明确跟踪requestExecutors
   *
   * @return whether the request is received.
   */
  @DeveloperApi
  override def killExecutor(executorId: String): Boolean = super.killExecutor(executorId)

  /**
   * Request that the cluster manager kill the specified executor without adjusting the
   * application resource requirements.
   * 请求群集管理器在不调整应用程序资源需求的条件下,杀死指定的执行器
   * The effect is that a new executor will be launched in place of the one killed by
   * this request. This assumes the cluster manager will automatically and eventually
   * 一个新的执行器将推出代替一个被这个请求杀死的执行器,
   * fulfill all missing application resource requests.
   * 这假设集群管理器将自动完成,并最终满足所有丢失的应用程序资源请求
   *
   * Note: The replace is by no means guaranteed; another application on the same cluster  
   * can steal the window of opportunity and acquire this application's resources in the
   * mean time.
   *
   * @return whether the request is received. 是否收到请求
   */
  private[spark] def killAndReplaceExecutor(executorId: String): Boolean = {
    schedulerBackend match {
      case b: CoarseGrainedSchedulerBackend =>
        b.killExecutors(Seq(executorId), replace = true)
      case _ =>
        logWarning("Killing executors is only supported in coarse-grained mode")
        false
    }
  }

  /** 
   *  The version of Spark on which this application is running. 
   *  应用程序正在运行Spark的版本
   *  */
  def version: String = SPARK_VERSION

  /**
   * Return a map from the slave to the max memory available for caching and the remaining
   * memory available for caching.
   * 返回一个从slave可用于缓存的最大内存以及可用于缓存的剩余内存
   */
  def getExecutorMemoryStatus: Map[String, (Long, Long)] = {
    assertNotStopped()
    env.blockManager.master.getMemoryStatus.map { case(blockManagerId, mem) =>
      (blockManagerId.host + ":" + blockManagerId.port, mem)
    }
  }

  /**
   * :: DeveloperApi ::
   * Return information about what RDDs are cached, if they are in mem or on disk, how much space
   * they take, etc.
    * 返回关于RDD被缓存的信息,如果它们在mem内存或磁盘上,它们占用多少空间等等。
   */
  @DeveloperApi
  def getRDDStorageInfo: Array[RDDInfo] = {
    assertNotStopped()
    val rddInfos = persistentRdds.values.map(RDDInfo.fromRdd).toArray
    StorageUtils.updateRddInfo(rddInfos, getExecutorStorageStatus)
    rddInfos.filter(_.isCached)
  }

  /**
   * Returns an immutable map of RDDs that have marked themselves as persistent via cache() call.
   * 返回一个不可变Map的RDDS,这标志着自己调用持久方法cache()
   * Note that this does not necessarily mean the caching or computation was successful.
   * 注意:这并不一定意味着缓存或计算是成功的
   */
  def getPersistentRDDs: Map[Int, RDD[_]] = persistentRdds.toMap

  /**
   * :: DeveloperApi ::
   * Return information about blocks stored in all of the slaves
   * 返回存储在从节点中所有块的信息
   */
  @DeveloperApi
  def getExecutorStorageStatus: Array[StorageStatus] = {
    assertNotStopped()
    env.blockManager.master.getStorageStatus
  }

  /**
   * :: DeveloperApi ::
   * Return pools for fair scheduler
   * 返回公平调度池
   */
  @DeveloperApi
  def getAllPools: Seq[Schedulable] = {
    assertNotStopped()
    // TODO(xiajunluan): We should take nested pools into account
    taskScheduler.rootPool.schedulableQueue.toSeq
  }

  /**
   * :: DeveloperApi ::
   * Return the pool associated with the given name, if one exists
   * 返回存在给定名称关联调度器
   */
  @DeveloperApi
  def getPoolForName(pool: String): Option[Schedulable] = {
    assertNotStopped()
    Option(taskScheduler.rootPool.schedulableNameToSchedulable.get(pool))
  }

  /**
   * Return current scheduling mode
   * 返回当前调度模式
   */
  def getSchedulingMode: SchedulingMode.SchedulingMode = {
    assertNotStopped()
    taskScheduler.schedulingMode
  }

  /**
   * Clear the job's list of files added by `addFile` so that they do not get downloaded to
   * any new nodes.
    * 清除“addFile”添加的作业列表,以使它们不被下载到任何新节点
   */
  @deprecated("adding files no longer creates local copies that need to be deleted", "1.0.0")
  def clearFiles() {
    addedFiles.clear()
  }

  /**
   * Gets the locality information associated with the partition in a particular rdd
   * 获取一个指定的RDD的分区有关的位置信息
   * @param rdd of interest 感兴趣的RDD
   * @param partition to be looked up for locality 查找位置分区
   * @return list of preferred locations for the partition 分区的首选位置列表
   */
  private [spark] def getPreferredLocs(rdd: RDD[_], partition: Int): Seq[TaskLocation] = {
    dagScheduler.getPreferredLocs(rdd, partition)
  }

  /**
   * Register an RDD to be persisted in memory and/or disk storage
   * 注册一个持久化的RDD存储内存或磁盘
   */
  private[spark] def persistRDD(rdd: RDD[_]) {
    persistentRdds(rdd.id) = rdd
  }

  /**
   * Unpersist an RDD from memory and/or disk storage
   * 在内存或磁盘删除一个RDD,参数blocking是否堵塞
   */
  private[spark] def unpersistRDD(rddId: Int, blocking: Boolean = true) {
    env.blockManager.master.removeRdd(rddId, blocking)
    persistentRdds.remove(rddId)//根据RDDID从HashMap删除RDD
    listenerBus.post(SparkListenerUnpersistRDD(rddId))//通知监听器
  }

  /**
   * Adds a JAR dependency for all tasks to be executed on this SparkContext in the future.
   * 增加所有任务要执行sparkcontext依赖JAR
   * The `path` passed can be either a local file, a file in HDFS (or other Hadoop-supported
   * filesystems), an HTTP, HTTPS or FTP URI, or local:/path for a file on every worker node.
   * 通过的“路径”可以是本地文件,在HDFS中文件, an HTTP, HTTPS or FTP URI,或本地:在每一个工作节点上的文件路径
   */
  def addJar(path: String) {
    if (path == null) {
      logWarning("null specified as parameter to addJar")
    } else {
      var key = ""
      if (path.contains("\\")) {
        // For local paths with backslashes on Windows, URI throws an exception
        //用反斜线Windows本地路径,URI抛出一个异常
        key = env.httpFileServer.addJar(new File(path))
      } else {
        val uri = new URI(path)
        key = uri.getScheme match {
          // A JAR file which exists only on the driver node
          //只有在驱动节点存在的jar文件
          case null | "file" =>
            // yarn-standalone is deprecated, but still supported
            //yarn-独立已过时,但仍然支持
            if (SparkHadoopUtil.get.isYarnMode() &&
                (master == "yarn-standalone" || master == "yarn-cluster")) {
              // In order for this to work in yarn-cluster mode the user must specify the
              // --addJars option to the client to upload the file into the distributed cache
              // of the AM to make it show up in the current working directory.
              //为了使其在yarn群集模式下工作,用户必须指定--addJars选项,
              // 客户端将文件上传到AM的分布式缓存中,使其显示在当前工作目录中。
              val fileName = new Path(uri.getPath).getName()
              try {
                env.httpFileServer.addJar(new File(fileName))
              } catch {
                case e: Exception =>
                  // For now just log an error but allow to go through so spark examples work.
                  // The spark examples don't really need the jar distributed since its also
                  // the app jar.
                  //现在只记录一个错误，但允许通过这样的Spark示例工作,Spark示例从它也不需要分发的jar
                  //应用程序jar
                  logError("Error adding jar (" + e + "), was the --addJars option used?")
                  null
              }
            } else {
              try {
                env.httpFileServer.addJar(new File(uri.getPath))
              } catch {
                case exc: FileNotFoundException =>
                  logError(s"Jar not found at $path")
                  null
                case e: Exception =>
                  // For now just log an error but allow to go through so spark examples work.
                  //现在只需要日志一个错误,但允许通过这样的Spark的例子工作
                  // The spark examples don't really need the jar distributed since its also
                  // the app jar.
                  //Spark的例子并不真的需要它的jar,因为它也是应用程序jar
                  logError("Error adding jar (" + e + "), was the --addJars option used?")
                  null
              }
            }
          // A JAR file which exists locally on every worker node
          //在每个工作节点存在一个jar文件
          case "local" =>
            "file:" + uri.getPath
          case _ =>
            path
        }
      }
      if (key != null) {
        addedJars(key) = System.currentTimeMillis
        logInfo("Added JAR " + path + " at " + key + " with timestamp " + addedJars(key))
      }
    }
    postEnvironmentUpdate()
  }

  /**
   * Clear the job's list of JARs added by `addJar` so that they do not get downloaded to
   * any new nodes.
    * 清除“addJar”添加的JAR作业列表,以便它们不被下载到任何新节点
   */
  @deprecated("adding jars no longer creates local copies that need to be deleted", "1.0.0")
  def clearJars() {
    addedJars.clear()
  }

  // Shut down the SparkContext.
  //关闭sparkcontext
  def stop() {
    // Use the stopping variable to ensure no contention for the stop scenario.
    //使用停止变量,确保停止没有竞争,
    // Still track the stopped variable for use elsewhere in the code.
    //跟踪停止使用代码中的其他变量
    if (!stopped.compareAndSet(false, true)) {
      logInfo("SparkContext already stopped.")
      return
    }
    if (_shutdownHookRef != null) {
      ShutdownHookManager.removeShutdownHook(_shutdownHookRef)
    }

    Utils.tryLogNonFatalError {
      postApplicationEnd()
    }
    Utils.tryLogNonFatalError {
      _ui.foreach(_.stop())
    }
    if (env != null) {
      Utils.tryLogNonFatalError {
        env.metricsSystem.report()
      }
    }
    if (metadataCleaner != null) {
      Utils.tryLogNonFatalError {
        metadataCleaner.cancel()
      }
    }
    Utils.tryLogNonFatalError {
      _cleaner.foreach(_.stop())
    }
    Utils.tryLogNonFatalError {
      _executorAllocationManager.foreach(_.stop())
    }
    if (_dagScheduler != null) {
      Utils.tryLogNonFatalError {
        _dagScheduler.stop()
      }
      _dagScheduler = null
    }
    if (_listenerBusStarted) {
      Utils.tryLogNonFatalError {
        listenerBus.stop()
        _listenerBusStarted = false
      }
    }
    Utils.tryLogNonFatalError {
      _eventLogger.foreach(_.stop())
    }
    if (env != null && _heartbeatReceiver != null) {
      Utils.tryLogNonFatalError {
        env.rpcEnv.stop(_heartbeatReceiver)
      }
    }
    Utils.tryLogNonFatalError {
      _progressBar.foreach(_.stop())
    }
    _taskScheduler = null
    // TODO: Cache.stop()?
    if (_env != null) {
      Utils.tryLogNonFatalError {
        _env.stop()
      }
      SparkEnv.set(null)
    }
    // Unset YARN mode system env variable, to allow switching between cluster types.
    //取消设置YARN模式系统env变量,以允许在集群类型之间切换
    System.clearProperty("SPARK_YARN_MODE")
    SparkContext.clearActiveContext()
    logInfo("Successfully stopped SparkContext")
  }


  /**
   * Get Spark's home location from either a value set through the constructor,
   * 获取SPARK_HOME的位置,来自一个构造函数设置的值或者spark.home属性,或者SPARK_HOME环境变量
   * or the spark.home Java property, or the SPARK_HOME environment variable
   * (in that order of preference). If neither of these is set, return None.
   * (按优先顺序).如果这两个都没有设置,返回没有
   * 
   */
  private[spark] def getSparkHome(): Option[String] = {
    conf.getOption("spark.home").orElse(Option(System.getenv("SPARK_HOME")))
  }

  /**
   * Set the thread-local property for overriding the call sites
   * of actions and RDDs.设置本地线程属性,覆盖RDD调用位置
   */
  def setCallSite(shortCallSite: String) {
    setLocalProperty(CallSite.SHORT_FORM, shortCallSite)
  }

  /**
   * Set the thread-local property for overriding the call sites
   * of actions and RDDs.
   * 设置本地线程属性,覆盖RDD调用位置
   */
  private[spark] def setCallSite(callSite: CallSite) {
    setLocalProperty(CallSite.SHORT_FORM, callSite.shortForm)
    setLocalProperty(CallSite.LONG_FORM, callSite.longForm)
  }

  /**
   * Clear the thread-local property for overriding the call sites
   * of actions and RDDs.清除本地线程属性,覆盖RDD调用位置
   */
  def clearCallSite() {
    setLocalProperty(CallSite.SHORT_FORM, null)
    setLocalProperty(CallSite.LONG_FORM, null)
  }

  /**
   * Capture the current user callsite and return a formatted version for printing. If the user  
   * has overridden the call site using `setCallSite()`, this will return the user's version.
   * 获取当前用户的调用点,并返回一个打印格式化版本,如果用户重写调用站点使用'setCallSite',这将返回用户的版本
   */
  private[spark] def getCallSite(): CallSite = {
    Option(getLocalProperty(CallSite.SHORT_FORM)).map { case shortCallSite =>
      val longCallSite = Option(getLocalProperty(CallSite.LONG_FORM)).getOrElse("")
      CallSite(shortCallSite, longCallSite)
    }.getOrElse(Utils.getCallSite())
  }

  /**
   * Run a function on a given set of partitions in an RDD and pass the results to the given
   * handler function. This is the main entry point for all actions in Spark.
    * 在RDD中给定的一组分区上运行一个函数,并将结果传递给给定的处理函数(resultHandler),这是Spark中所有操作的主要入口点
   */
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      resultHandler: (Int, U) => Unit): Unit = {
    if (stopped.get()) {
      throw new IllegalStateException("SparkContext has been shutdown")
    }
    val callSite = getCallSite
    val cleanedFunc = clean(func)
    logInfo("Starting job: " + callSite.shortForm)
    if (conf.getBoolean("spark.logLineage", false)) {
      logInfo("RDD's recursive dependencies:\n" + rdd.toDebugString)
    }
    dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, resultHandler, localProperties.get)
    progressBar.foreach(_.finishAll())
    rdd.doCheckpoint()
  }

  /**
   * Run a function on a given set of partitions in an RDD and return the results as an array.
   * 在RDD中给定的一组分区上运行函数,并将结果作为数组返回。
   */
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int]): Array[U] = {
    val results = new Array[U](partitions.size)
    runJob[T, U](rdd, func, partitions, (index, res) => results(index) = res)
    results
  }

  /**
   * Run a job on a given set of partitions of an RDD, but take a function of type
   * `Iterator[T] => U` instead of `(TaskContext, Iterator[T]) => U`.
   *在RDD的一组给定的分区上运行作业Job,但是一个函数类型的迭代器Iterator [T] => U,而不是(TaskContext,Iterator [T])=> U的函数
    * 在RDD的一组给定的分区上运行作业
   */
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: Iterator[T] => U,
      partitions: Seq[Int]): Array[U] = {
    val cleanedFunc = clean(func)
    runJob(rdd, (ctx: TaskContext, it: Iterator[T]) => cleanedFunc(it), partitions)
  }


  /**
   * Run a function on a given set of partitions in an RDD and pass the results to the given
   * handler function. This is the main entry point for all actions in Spark.
    * 在RDD中给定的一组分区上运行一个函数,并将结果传递给给定的处理函数,这是Spark中所有操作的主要入口点
   *
   * The allowLocal flag is deprecated as of Spark 1.5.0+.
   */
  @deprecated("use the version of runJob without the allowLocal parameter", "1.5.0")
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      allowLocal: Boolean,
      resultHandler: (Int, U) => Unit): Unit = {
    if (allowLocal) {
      logWarning("sc.runJob with allowLocal=true is deprecated in Spark 1.5.0+")
    }
    runJob(rdd, func, partitions, resultHandler)
  }

  /**
   * Run a function on a given set of partitions in an RDD and return the results as an array.
   *
   * The allowLocal flag is deprecated as of Spark 1.5.0+.
   */
  @deprecated("use the version of runJob without the allowLocal parameter", "1.5.0")
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      allowLocal: Boolean
      ): Array[U] = {
    if (allowLocal) {
      logWarning("sc.runJob with allowLocal=true is deprecated in Spark 1.5.0+")
    }
    runJob(rdd, func, partitions)
  }

  /**
   * Run a job on a given set of partitions of an RDD, but take a function of type
   * `Iterator[T] => U` instead of `(TaskContext, Iterator[T]) => U`.
   *
   * The allowLocal argument is deprecated as of Spark 1.5.0+.
   */
  @deprecated("use the version of runJob without the allowLocal parameter", "1.5.0")
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: Iterator[T] => U,
      partitions: Seq[Int],
      allowLocal: Boolean
      ): Array[U] = {
    if (allowLocal) {
      logWarning("sc.runJob with allowLocal=true is deprecated in Spark 1.5.0+")
    }
    runJob(rdd, func, partitions)
  }

  /**
   * Run a job on all partitions in an RDD and return the results in an array.
    * 在RDD中的所有分区上运行作业Job,并将结果返回到数组中
   */
  def runJob[T, U: ClassTag](rdd: RDD[T], func: (TaskContext, Iterator[T]) => U): Array[U] = {
    runJob(rdd, func, 0 until rdd.partitions.length)
  }

  /**
   * Run a job on all partitions in an RDD and return the results in an array.
    * 在RDD中的所有分区上运行作业Job,并将结果返回到数组中
   */
  def runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): Array[U] = {
    runJob(rdd, func, 0 until rdd.partitions.length)
  }

  /**
   * Run a job on all partitions in an RDD and pass the results to a handler function.
    * 在RDD中的所有分区上运行作业Job,并将结果传递给处理函数
   */
  def runJob[T, U: ClassTag](
    rdd: RDD[T],
    processPartition: (TaskContext, Iterator[T]) => U,
    resultHandler: (Int, U) => Unit)
  {
    runJob[T, U](rdd, processPartition, 0 until rdd.partitions.length, resultHandler)
  }

  /**
   * Run a job on all partitions in an RDD and pass the results to a handler function.
   * 在RDD中的所有分区上运行作业Job,并将结果传递给处理函数
   */
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      processPartition: Iterator[T] => U,
      resultHandler: (Int, U) => Unit)
  {
    val processFunc = (context: TaskContext, iter: Iterator[T]) => processPartition(iter)
    runJob[T, U](rdd, processFunc, 0 until rdd.partitions.length, resultHandler)
  }

  /**
   * :: DeveloperApi ::
   * Run a job that can return approximate results.
   * 运行可以返回近似结果的作业
   */
  @DeveloperApi
  def runApproximateJob[T, U, R](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      evaluator: ApproximateEvaluator[U, R],
      timeout: Long): PartialResult[R] = {
    assertNotStopped()
    val callSite = getCallSite
    logInfo("Starting job: " + callSite.shortForm)
    val start = System.nanoTime
    val cleanedFunc = clean(func)
    val result = dagScheduler.runApproximateJob(rdd, cleanedFunc, evaluator, callSite, timeout,
      localProperties.get)
    logInfo(
      "Job finished: " + callSite.shortForm + ", took " + (System.nanoTime - start) / 1e9 + " s")
    result
  }

  /**
   * :: Experimental ::
   * Submit a job for execution and return a FutureJob holding the result.
   * 提交一个作业执行并返回一个FutureJob持有的结果
   */
  @Experimental
  def submitJob[T, U, R](
      rdd: RDD[T],
      processPartition: Iterator[T] => U,
      partitions: Seq[Int],
      resultHandler: (Int, U) => Unit,
      resultFunc: => R): SimpleFutureAction[R] =
  {
    assertNotStopped()
    val cleanF = clean(processPartition)
    val callSite = getCallSite
    val waiter = dagScheduler.submitJob(
      rdd,
      (context: TaskContext, iter: Iterator[T]) => cleanF(iter),
      partitions,
      callSite,
      resultHandler,
      localProperties.get)
    new SimpleFutureAction(waiter, resultFunc)
  }

  /**
   * Cancel active jobs for the specified group. See [[org.apache.spark.SparkContext.setJobGroup]]
   * for more information.
   * 取消指定组的活动作业
   */
  def cancelJobGroup(groupId: String) {
    assertNotStopped()
    dagScheduler.cancelJobGroup(groupId)
  }

  /** 
  *Cancel all jobs that have been scheduled or are running.  
  *取消已计划或正在运行的所有作业
  */
  def cancelAllJobs() {
    assertNotStopped()
    dagScheduler.cancelAllJobs()
  }

  /** 
  * Cancel a given job if it's scheduled or running 
  * 取消一个指定的Job,如果它在计划或正在运行
  */
  private[spark] def cancelJob(jobId: Int) {
    dagScheduler.cancelJob(jobId)
  }

  /** 
  * Cancel a given stage and all jobs associated with it 
  * 取消一个给定的阶段和与它相关联的所有工作
  */
  private[spark] def cancelStage(stageId: Int) {
    dagScheduler.cancelStage(stageId)
  }

  /**
   * Clean a closure to make it ready to serialized and send to tasks
   * 清理一个关闭使它准备序列化并发送到任务
   * (removes unreferenced variables in $outer's, updates REPL variables)
   * If <tt>checkSerializable</tt> is set, <tt>clean</tt> will also proactively
   * check to see if <tt>f</tt> is serializable and throw a <tt>SparkException</tt>
   * if not.
   *
   * @param f the closure to clean 清理闭包
   * @param checkSerializable whether or not to immediately check <tt>f</tt> for serializability
   * @throws SparkException if <tt>checkSerializable</tt> is set but <tt>f</tt> is not
   *   serializable
   *   ClosureCleaner清除闭包中的不能序列化的变量,防止RDD在网络传输过程中反序列化失败
   */
  private[spark] def clean[F <: AnyRef](f: F, checkSerializable: Boolean = true): F = {
    ClosureCleaner.clean(f, checkSerializable)
    f
  }

  /**
   * Set the directory under which RDDs are going to be checkpointed. The directory must
   * be a HDFS path if running on a cluster.
   * 设置目录下的RDDS检查点,该目录必须在集群上运行的一个HDFS路径。
   */
  def setCheckpointDir(directory: String) {

    // If we are running on a cluster, log a warning if the directory is local.
    //如果我们在一个集群上运行,如果目录是本地,则警告日志
    // Otherwise, the driver may attempt to reconstruct the checkpointed RDD from
    //否则,驱动器可能尝试从自己的本地文件系统RDD重建检查点
    // its own local file system, which is incorrect because the checkpoint files
    // are actually on the executor machines.
    //这是不正确的,因为检查点文件实际上是在执行器机器上
    if (!isLocal && Utils.nonLocalPaths(directory).isEmpty) {
      logWarning("Checkpoint directory must be non-local " +
        "if Spark is running on a cluster: " + directory)
    }

    checkpointDir = Option(directory).map { dir =>
      val path = new Path(dir, UUID.randomUUID().toString)
      val fs = path.getFileSystem(hadoopConfiguration)
      fs.mkdirs(path)
      //FileStatus对象封装了文件系统中文件和目录的元数据,包括文件的长度、块大小、备份数、修改时间、所有者以及权限等信息
      fs.getFileStatus(path).getPath.toString
    }
  }

  def getCheckpointDir: Option[String] = checkpointDir

  /** 
  * Default level of parallelism to use when not given by user (e.g. parallelize and makeRDD). 
  * 默认并发数
  */
  def defaultParallelism: Int = {
    assertNotStopped()
    taskScheduler.defaultParallelism
  }

  /** Default min number of partitions for Hadoop RDDs when not given by user
    * 用户未给出Hadoop RDD的默认分区数*/
  @deprecated("use defaultMinPartitions", "1.0.0")
  //math.min返回指定的数字中带有最低值的数字
  def defaultMinSplits: Int = math.min(defaultParallelism, 2)

  /**
   * Default min number of partitions for Hadoop RDDs when not given by user
   * Hadoop默认最小RDDS分区数
   * Notice that we use math.min so the "defaultMinPartitions" cannot be higher than 2.
   * The reasons for this are discussed in https://github.com/mesos/spark/pull/718
   */
  //math.min返回指定的数字中带有最低值的数字
  def defaultMinPartitions: Int = math.min(defaultParallelism, 2)

  private val nextShuffleId = new AtomicInteger(0)

  private[spark] def newShuffleId(): Int = nextShuffleId.getAndIncrement()
  //AtomicInteger,一个提供原子操作的Integer的类,++i和i++操作并不是线程安全的,不可避免的会用到synchronized关键字。
  //而AtomicInteger则通过一种线程安全的加减操作接口
  private val nextRddId = new AtomicInteger(0)

  /** 
   *  Register a new RDD, returning its RDD ID 
   *  注册一个新的RDD,返回一个RDD ID标识
   *  */
  private[spark] def newRddId(): Int = nextRddId.getAndIncrement()

  /**
   * Registers listeners specified in spark.extraListeners, then starts the listener bus.
   * 在spark.extraListeners中注册一个监听,然后启动侦听器总线
   * This should be called after all internal listeners have been registered with the listener bus
   * 这应该被调用后,所有的内部侦听器已被注册的侦听器总线
   * (e.g. after the web UI and event logging listeners have been registered).
   *
   */
  private def setupAndStartListenerBus(): Unit = {
    // Use reflection to instantiate listeners specified via `spark.extraListeners`
    //通过指定`spark.extraListeners`使用反射来实例化监听器
    try {
      val listenerClassNames: Seq[String] =
        conf.get("spark.extraListeners", "").split(',').map(_.trim).filter(_ != "")
      for (className <- listenerClassNames) {
        // Use reflection to find the right constructor
        //使用反射找到正确的构造函数
        val constructors = {
          val listenerClass = Utils.classForName(className)
          listenerClass.getConstructors.asInstanceOf[Array[Constructor[_ <: SparkListener]]]
        }
        val constructorTakingSparkConf = constructors.find { c =>
          c.getParameterTypes.sameElements(Array(classOf[SparkConf]))
        }
        lazy val zeroArgumentConstructor = constructors.find { c =>
          c.getParameterTypes.isEmpty
        }
        val listener: SparkListener = {
          if (constructorTakingSparkConf.isDefined) {
            constructorTakingSparkConf.get.newInstance(conf)
          } else if (zeroArgumentConstructor.isDefined) {
            zeroArgumentConstructor.get.newInstance()
          } else {
            throw new SparkException(
              s"$className did not have a zero-argument constructor or a" +
                " single-argument constructor that accepts SparkConf. Note: if the class is" +
                " defined inside of another Scala class, then its constructors may accept an" +
                " implicit parameter that references the enclosing class; in this case, you must" +
                " define the listener as a top-level class in order to prevent this extra" +
                " parameter from breaking Spark's ability to find a valid constructor.")
          }
        }
        listenerBus.addListener(listener)
        logInfo(s"Registered listener $className")
      }
    } catch {
      case e: Exception =>
        try {
          stop()
        } finally {
          throw new SparkException(s"Exception when registering SparkListener", e)
        }
    }

    listenerBus.start(this)
    _listenerBusStarted = true
  }

  /** 
   *  Post the application start event
   *  提交应用程序启动事件
   *   */
  private def postApplicationStart() {
    // Note: this code assumes that the task scheduler has been initialized and has contacted
    // the cluster manager to get an application ID (in case the cluster manager provides one).
    //注意：此代码假定任务调度程序已初始化,并已触发集群管理器获取应用程序ID(如果集群管理器提供一个)
    listenerBus.post(SparkListenerApplicationStart(appName, Some(applicationId),
      startTime, sparkUser, applicationAttemptId, schedulerBackend.getDriverLogUrls))
  }

  /** 
   *  Post the application end event 
   *  提交应用程序结束事件
   *  */
  private def postApplicationEnd() {
    listenerBus.post(SparkListenerApplicationEnd(System.currentTimeMillis))
  }

  /** 
   *  Post the environment update event once the task scheduler is ready 
   *  一旦任务调度程序准备就绪后,发布环境更新事件
   *  */
  private def postEnvironmentUpdate() {
    if (taskScheduler != null) {
      val schedulingMode = getSchedulingMode.toString
      val addedJarPaths = addedJars.keys.toSeq
      val addedFilePaths = addedFiles.keys.toSeq
      val environmentDetails = SparkEnv.environmentDetails(conf, schedulingMode, addedJarPaths,
        addedFilePaths)
      val environmentUpdate = SparkListenerEnvironmentUpdate(environmentDetails)
      listenerBus.post(environmentUpdate)
    }
  }

  /** Called by MetadataCleaner to clean up the persistentRdds map periodically */
  //由MetadataCleaner调用,定期清理persistentRdds映射
  private[spark] def cleanup(cleanupTime: Long) {
    //清除RDD过期内容
    persistentRdds.clearOldValues(cleanupTime)
  }

  // In order to prevent multiple SparkContexts from being active at the same time, mark this
  // context as having finished construction.
  // NOTE: this must be placed at the end of the SparkContext constructor.
  //SparkContext初始化的最后将当前SparkContext的状态从contextBeingConstructed构建中更改为已激活
  SparkContext.setActiveContext(this, allowMultipleContexts)
}

/**
 * The SparkContext object contains a number of implicit conversions and parameters for use with 
 * various Spark features.
 * Sparkcontext对象包含用于各种Spark特征隐式转换和参数。
 */
object SparkContext extends Logging {

  /**
   * Lock that guards access to global variables that track SparkContext construction.
   * 锁保护访问全局变量,跟踪sparkcontext构造
   */
  private val SPARK_CONTEXT_CONSTRUCTOR_LOCK = new Object()

  /**
   * The active, fully-constructed SparkContext.  If no SparkContext is active, then this is `null`.
   * 这激活,全面构造一个SparkContext,如果没有sparkcontext活跃,那么这是null,
   * Access to this field is guarded by SPARK_CONTEXT_CONSTRUCTOR_LOCK.
   * 访问这个字段是由SPARK_CONTEXT_CONSTRUCTOR_LOCK
   * AtomicReference则对应普通的对象引用,它可以保证你在修改对象引用时的线程安全性.
   */
  private val activeContext: AtomicReference[SparkContext] =
    new AtomicReference[SparkContext](null)

  /**
   * Points to a partially-constructed SparkContext if some thread is in the SparkContext
   * constructor, or `None` if no SparkContext is being constructed.
    *
   * Access to this field is guarded by SPARK_CONTEXT_CONSTRUCTOR_LOCK
    *
    *如果某些线程在SparkContext中,则指向部分构造的SparkContext构造函数,如果没有构造SparkContext,则为“None”。
    *访问此字段由SPARK_CONTEXT_CONSTRUCTOR_LOCK保护
   */
  private var contextBeingConstructed: Option[SparkContext] = None

  /**
   * Called to ensure that no other SparkContext is running in this JVM.
   * 为确保没有其他sparkcontext在JVM中运行
   * Throws an exception if a running context is detected and logs a warning if another thread is
   * constructing a SparkContext.  This warning is necessary because the current locking scheme
   * prevents us from reliably distinguishing between cases where another context is being
   * constructed and cases where another constructor threw an exception.
   *  如果检测到运行的上下文,则抛出异常,如果另一个线程正在构建SparkContext,则会发出警告,此警告是必要的,
    * 因为当前的锁定方案阻止我们可靠地区分正在构建另一个上下文的情况和另一个构造函数抛出异常的情况。
    * 如果另一个线程正在构造则抛出异常,
    * assert没有其他上下文正在运行
   */
  private def assertNoOtherContextIsRunning(
      sc: SparkContext,
      allowMultipleContexts: Boolean): Unit = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      contextBeingConstructed.foreach { otherContext =>
        if (otherContext ne sc) {  // checks for reference equality 检查引用相等
          // Since otherContext might point to a partially-constructed context, guard against
          // its creationSite field being null:
          //由于otherContext可能指向部分构造的上下文,因此防范其creationSite字段为null：
          val otherContextCreationSite =
            Option(otherContext.creationSite).map(_.longForm).getOrElse("unknown location")
          val warnMsg = "Another SparkContext is being constructed (or threw an exception in its" +
            " constructor).  This may indicate an error, since only one SparkContext may be" +
            " running in this JVM (see SPARK-2243)." +
            s" The other SparkContext was created at:\n$otherContextCreationSite"
          logWarning(warnMsg)
        }

        if (activeContext.get() != null) {
          val ctx = activeContext.get()
          val errMsg = "Only one SparkContext may be running in this JVM (see SPARK-2243)." +
            " To ignore this error, set spark.driver.allowMultipleContexts = true. " +
            s"The currently running SparkContext was created at:\n${ctx.creationSite.longForm}"
          val exception = new SparkException(errMsg)
          if (allowMultipleContexts) {
            logWarning("Multiple running SparkContexts detected in the same JVM!", exception)
          } else {
            throw exception
          }
        }
      }
    }
  }

  /**
   * This function may be used to get or instantiate a SparkContext and register it as a
   * singleton object. Because we can only have one active SparkContext per JVM,
   * this is useful when applications may wish to share a SparkContext.
   *
   * 这个函数用来获取或实例化单个sparkcontext,每个JVM只有一个SparkContext激活,使用的时候可能与应用程序共享SparkContext
    *
   * Note: This function cannot be used to create multiple SparkContext instances
   * even if multiple contexts are allowed.
    *注意：即使允许多个上下文,此函数也不能用于创建多个SparkContext实例
   */
  def getOrCreate(config: SparkConf): SparkContext = {
    // Synchronize to ensure that multiple create requests don't trigger an exception
    // from assertNoOtherContextIsRunning within setActiveContext
    //同步以确保多个创建请求不会从setActiveContext中的assertNoOtherContextIsRunning触发异常
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      if (activeContext.get() == null) {
        setActiveContext(new SparkContext(config), allowMultipleContexts = false)
      }
      activeContext.get()
    }
  }

  /**
   * This function may be used to get or instantiate a SparkContext and register it as a
   * singleton object. Because we can only have one active SparkContext per JVM,
   * this is useful when applications may wish to share a SparkContext.
    *
    * 此函数可用于获取或实例化SparkContext并将其注册为单对象,因为我们每个JVM只能有一个活动的SparkContext
    * 当应用程序可能希望共享一个SparkContext时,这很有用
   *
   * This method allows not passing a SparkConf (useful if just retrieving).
    * 此方法允许不传递SparkConf(仅在检索时有用)
   *
   * Note: This function cannot be used to create multiple SparkContext instances
   * even if multiple contexts are allowed.
    *
    * 注意：即使允许多个上下文,此功能也不能用于创建多个SparkContext实例,
   */
  def getOrCreate(): SparkContext = {
    getOrCreate(new SparkConf())
  }

  /**
   * Called at the beginning of the SparkContext constructor to ensure that no SparkContext is
   * running.  Throws an exception if a running context is detected and logs a warning if another
   * thread is constructing a SparkContext.  This warning is necessary because the current locking
   * scheme prevents us from reliably distinguishing between cases where another context is being
   * constructed and cases where another constructor threw an exception.
    *
    * 在SparkContext构造函数的开头调用,以确保没有SparkContext正在运行,如果检测到运行的上下文,则抛出异常,
    * 如果另一个线程正在构建SparkContext,则会发出警告,此警告是必要的,
    * 因为当前的锁定方案阻止我们可靠地区分正在构建另一个上下文的情况和另一个构造函数抛出异常的情况。
    *
   * 确保实例的唯一性,并将当前SparkContext标记为正在构建中.
   */
  private[spark] def markPartiallyConstructed(
      sc: SparkContext,
      allowMultipleContexts: Boolean): Unit = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      assertNoOtherContextIsRunning(sc, allowMultipleContexts)
      contextBeingConstructed = Some(sc)//标示SparkContext正在构建中
    }
  }

  /**
   * Called at the end of the SparkContext constructor to ensure that no other SparkContext has
   * raced with this constructor and started.
   * SparkContext初始化的最后将当前SparkContext的状态从contextBeingConstructed构建中更改为已激活
   */
  private[spark] def setActiveContext(
      sc: SparkContext,
      allowMultipleContexts: Boolean): Unit = {
    //锁保护访问全局变量,跟踪sparkcontext构造
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      //为确保没有其他sparkcontext在JVM中运行
      assertNoOtherContextIsRunning(sc, allowMultipleContexts)
      //非None,sparkcontext正在建设
      contextBeingConstructed = None
      //
      activeContext.set(sc)
    }
  }

  /**
   * Clears the active SparkContext metadata.  This is called by `SparkContext#stop()`.  It's
   * also called in unit tests to prevent a flood of warnings from test suites that don't / can't
   * properly clean up their SparkContexts.
    *
    * 清除活动的SparkContext元数据,这由`SparkContext＃stop（）`调用,
    * 它的也在单元测试中调用,以防止来自没有/不能正确清理SparkContexts的测试套件的大量警告
    * 清除活动的sparkcontext元数据,这调用SparkContext#stop(),
   */
  private[spark] def clearActiveContext(): Unit = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      activeContext.set(null)
    }
  }

  private[spark] val SPARK_JOB_DESCRIPTION = "spark.job.description"
  private[spark] val SPARK_JOB_GROUP_ID = "spark.jobGroup.id"
  private[spark] val SPARK_JOB_INTERRUPT_ON_CANCEL = "spark.job.interruptOnCancel"
  private[spark] val RDD_SCOPE_KEY = "spark.rdd.scope"
  private[spark] val RDD_SCOPE_NO_OVERRIDE_KEY = "spark.rdd.scope.noOverride"

  /**
   * Executor id for the driver.  In earlier versions of Spark, this was `<driver>`, but this was
   * 驱动程序的执行器标识,在早期Spark版本,
   * changed to `driver` because the angle brackets caused escaping issues in URLs and XML (see
   * SPARK-6716 for more details).
   */
  private[spark] val DRIVER_IDENTIFIER = "driver"

  /**
   * Legacy version of DRIVER_IDENTIFIER, retained for backwards-compatibility.
   * 旧版本DRIVER_IDENTIFIER,保留向后兼容性
   */
  private[spark] val LEGACY_DRIVER_IDENTIFIER = "<driver>"

  // The following deprecated objects have already been copied to `object AccumulatorParam` to
  // make the compiler find them automatically. They are duplicate codes only for backward
  // compatibility, please update `object AccumulatorParam` accordingly if you plan to modify the
  // following ones.

  @deprecated("Replaced by implicit objects in AccumulatorParam. This is kept here only for " +
    "backward compatibility.", "1.3.0")
  object DoubleAccumulatorParam extends AccumulatorParam[Double] {
    def addInPlace(t1: Double, t2: Double): Double = t1 + t2
    def zero(initialValue: Double): Double = 0.0
  }

  @deprecated("Replaced by implicit objects in AccumulatorParam. This is kept here only for " +
    "backward compatibility.", "1.3.0")
  object IntAccumulatorParam extends AccumulatorParam[Int] {
    def addInPlace(t1: Int, t2: Int): Int = t1 + t2
    def zero(initialValue: Int): Int = 0
  }

  @deprecated("Replaced by implicit objects in AccumulatorParam. This is kept here only for " +
    "backward compatibility.", "1.3.0")
  object LongAccumulatorParam extends AccumulatorParam[Long] {
    def addInPlace(t1: Long, t2: Long): Long = t1 + t2
    def zero(initialValue: Long): Long = 0L
  }

  @deprecated("Replaced by implicit objects in AccumulatorParam. This is kept here only for " +
    "backward compatibility.", "1.3.0")
  object FloatAccumulatorParam extends AccumulatorParam[Float] {
    def addInPlace(t1: Float, t2: Float): Float = t1 + t2
    def zero(initialValue: Float): Float = 0f
  }

  // The following deprecated functions have already been moved to `object RDD` to
  // make the compiler find them automatically. They are still kept here for backward compatibility
  // and just call the corresponding functions in `object RDD`.

  @deprecated("Replaced by implicit functions in the RDD companion object. This is " +
    "kept here only for backward compatibility.", "1.3.0")
  def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)])
      (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairRDDFunctions[K, V] =
    RDD.rddToPairRDDFunctions(rdd)

  @deprecated("Replaced by implicit functions in the RDD companion object. This is " +
    "kept here only for backward compatibility.", "1.3.0")
  def rddToAsyncRDDActions[T: ClassTag](rdd: RDD[T]): AsyncRDDActions[T] =
    RDD.rddToAsyncRDDActions(rdd)

  @deprecated("Replaced by implicit functions in the RDD companion object. This is " +
    "kept here only for backward compatibility.", "1.3.0")
  def rddToSequenceFileRDDFunctions[K <% Writable: ClassTag, V <% Writable: ClassTag](
      rdd: RDD[(K, V)]): SequenceFileRDDFunctions[K, V] = {
    val kf = implicitly[K => Writable]
    val vf = implicitly[V => Writable]
    // Set the Writable class to null and `SequenceFileRDDFunctions` will use Reflection to get it
    //将Writable类设置为null,并且“SequenceFileRDDFunctions”将使用Reflection来获取它
    implicit val keyWritableFactory = new WritableFactory[K](_ => null, kf)
    implicit val valueWritableFactory = new WritableFactory[V](_ => null, vf)
    RDD.rddToSequenceFileRDDFunctions(rdd)
  }

  @deprecated("Replaced by implicit functions in the RDD companion object. This is " +
    "kept here only for backward compatibility.", "1.3.0")
  def rddToOrderedRDDFunctions[K : Ordering : ClassTag, V: ClassTag](
      rdd: RDD[(K, V)]): OrderedRDDFunctions[K, V, (K, V)] =
    RDD.rddToOrderedRDDFunctions(rdd)

  @deprecated("Replaced by implicit functions in the RDD companion object. This is " +
    "kept here only for backward compatibility.", "1.3.0")
  def doubleRDDToDoubleRDDFunctions(rdd: RDD[Double]): DoubleRDDFunctions =
    RDD.doubleRDDToDoubleRDDFunctions(rdd)

  @deprecated("Replaced by implicit functions in the RDD companion object. This is " +
    "kept here only for backward compatibility.", "1.3.0")
  def numericRDDToDoubleRDDFunctions[T](rdd: RDD[T])(implicit num: Numeric[T]): DoubleRDDFunctions =
    RDD.numericRDDToDoubleRDDFunctions(rdd)

  // The following deprecated functions have already been moved to `object WritableFactory` to
  // make the compiler find them automatically. They are still kept here for backward compatibility.

  @deprecated("Replaced by implicit functions in the WritableFactory companion object. This is " +
    "kept here only for backward compatibility.", "1.3.0")
  implicit def intToIntWritable(i: Int): IntWritable = new IntWritable(i)

  @deprecated("Replaced by implicit functions in the WritableFactory companion object. This is " +
    "kept here only for backward compatibility.", "1.3.0")
  implicit def longToLongWritable(l: Long): LongWritable = new LongWritable(l)

  @deprecated("Replaced by implicit functions in the WritableFactory companion object. This is " +
    "kept here only for backward compatibility.", "1.3.0")
  implicit def floatToFloatWritable(f: Float): FloatWritable = new FloatWritable(f)

  @deprecated("Replaced by implicit functions in the WritableFactory companion object. This is " +
    "kept here only for backward compatibility.", "1.3.0")
  implicit def doubleToDoubleWritable(d: Double): DoubleWritable = new DoubleWritable(d)

  @deprecated("Replaced by implicit functions in the WritableFactory companion object. This is " +
    "kept here only for backward compatibility.", "1.3.0")
  implicit def boolToBoolWritable (b: Boolean): BooleanWritable = new BooleanWritable(b)

  @deprecated("Replaced by implicit functions in the WritableFactory companion object. This is " +
    "kept here only for backward compatibility.", "1.3.0")
  implicit def bytesToBytesWritable (aob: Array[Byte]): BytesWritable = new BytesWritable(aob)

  @deprecated("Replaced by implicit functions in the WritableFactory companion object. This is " +
    "kept here only for backward compatibility.", "1.3.0")
  implicit def stringToText(s: String): Text = new Text(s)

  private implicit def arrayToArrayWritable[T <% Writable: ClassTag](arr: Traversable[T])
    : ArrayWritable = {
    def anyToWritable[U <% Writable](u: U): Writable = u

    new ArrayWritable(classTag[T].runtimeClass.asInstanceOf[Class[Writable]],
        arr.map(x => anyToWritable(x)).toArray)
  }

  // The following deprecated functions have already been moved to `object WritableConverter` to
  // make the compiler find them automatically. They are still kept here for backward compatibility
  // and just call the corresponding functions in `object WritableConverter`.
  // 以下已弃用的函数已被移动到`Object WritableConverter'，使编译器自动找到它们,
  // 它们仍然保留在这里用于向后兼容，并且在“对象WritableConverter”中调用相应的函数,

  @deprecated("Replaced by implicit functions in WritableConverter. This is kept here only for " +
    "backward compatibility.", "1.3.0")
  def intWritableConverter(): WritableConverter[Int] =
    WritableConverter.intWritableConverter()

  @deprecated("Replaced by implicit functions in WritableConverter. This is kept here only for " +
    "backward compatibility.", "1.3.0")
  def longWritableConverter(): WritableConverter[Long] =
    WritableConverter.longWritableConverter()

  @deprecated("Replaced by implicit functions in WritableConverter. This is kept here only for " +
    "backward compatibility.", "1.3.0")
  def doubleWritableConverter(): WritableConverter[Double] =
    WritableConverter.doubleWritableConverter()

  @deprecated("Replaced by implicit functions in WritableConverter. This is kept here only for " +
    "backward compatibility.", "1.3.0")
  def floatWritableConverter(): WritableConverter[Float] =
    WritableConverter.floatWritableConverter()

  @deprecated("Replaced by implicit functions in WritableConverter. This is kept here only for " +
    "backward compatibility.", "1.3.0")
  def booleanWritableConverter(): WritableConverter[Boolean] =
    WritableConverter.booleanWritableConverter()

  @deprecated("Replaced by implicit functions in WritableConverter. This is kept here only for " +
    "backward compatibility.", "1.3.0")
  def bytesWritableConverter(): WritableConverter[Array[Byte]] =
    WritableConverter.bytesWritableConverter()

  @deprecated("Replaced by implicit functions in WritableConverter. This is kept here only for " +
    "backward compatibility.", "1.3.0")
  def stringWritableConverter(): WritableConverter[String] =
    WritableConverter.stringWritableConverter()

  @deprecated("Replaced by implicit functions in WritableConverter. This is kept here only for " +
    "backward compatibility.", "1.3.0")
  def writableWritableConverter[T <: Writable](): WritableConverter[T] =
    WritableConverter.writableWritableConverter()

  /**
   * Find the JAR from which a given class was loaded, to make it easy for users to pass
   * their JARs to SparkContext.
   * 找到给定加载的类JAR名称,使用户可以轻松地将其JAR传递给SparkContext
   */
  def jarOfClass(cls: Class[_]): Option[String] = {
    val uri = cls.getResource("/" + cls.getName.replace('.', '/') + ".class")
    if (uri != null) {
      val uriStr = uri.toString
      if (uriStr.startsWith("jar:file:")) {
        // URI will be of the form "jar:file:/path/foo.jar!/package/cls.class",
        // so pull out the /path/foo.jar
        //URI将是“jar：file：/path/foo.jar！/package/cls.class”的形式,所以取出/path/foo.jar
        // println("==="+"jar:file:".length+"==="+uriStr.indexOf('!'))
        //===9===22,/path/foo.jar
        Some(uriStr.substring("jar:file:".length, uriStr.indexOf('!')))
      } else {
        None
      }
    } else {
      None
    }
  }

  /**
   * Find the JAR that contains the class of a particular object, to make it easy for users
   * to pass their JARs to SparkContext. In most cases you can call jarOfObject(this) in
   * your driver program.
   * 找到包含特定对象类的JAR,以便用户可以将其JAR传递给SparkContext,在大多数情况下,您可以在驱动程序中调用jarOfObject(this)。
   */
  def jarOfObject(obj: AnyRef): Option[String] = jarOfClass(obj.getClass)

  /**
    * 使用可以单独传递给SparkContext的参数,创建SparkConf的修改版本
    * Creates a modified version of a SparkConf with the parameters that can be passed separately
   * to SparkContext, to make it easier to write SparkContext's constructors. This ignores
   * 以便更容易地编写SparkContext的构造函数,这忽略了作为空的默认值传递的参数,
   * parameters that are passed as the default value of null, instead of throwing an exception
   * like SparkConf would.
    * 而不是像SparkConf那样抛出一个异常
   */
  private[spark] def updatedConf(
      conf: SparkConf,
      master: String,
      appName: String,
      sparkHome: String = null,
      jars: Seq[String] = Nil,//列表结尾为Nil
      environment: Map[String, String] = Map()): SparkConf =
  {
    val res = conf.clone()
    res.setMaster(master)
    res.setAppName(appName)
    if (sparkHome != null) {
      res.setSparkHome(sparkHome)
    }
    if (jars != null && !jars.isEmpty) {
      res.setJars(jars)
    }
    res.setExecutorEnv(environment.toSeq)
    res
  }

  /**
   * The number of driver cores to use for execution in local mode, 0 otherwise.
   * 本地模式下的执行器内核的数,否则为0
   */
  private[spark] def numDriverCores(master: String): Int = {
    def convertToInt(threads: String): Int = {
      //Runtime.getRuntime().availableProcessors()方法获得当前设备的CPU个数
      if (threads == "*") Runtime.getRuntime.availableProcessors() else threads.toInt
    }
    master match {
      case "local" => 1
      case SparkMasterRegex.LOCAL_N_REGEX(threads) => convertToInt(threads)
      case SparkMasterRegex.LOCAL_N_FAILURES_REGEX(threads, _) => convertToInt(threads)
      case _ => 0 // driver is not used for execution 驱动器不用于执行
    }
  }

  /**
   * Create a task scheduler based on a given master URL.
   * 基于一个给定的主节点地址创建一个任务调度程序,
   * Return a 2-tuple of the scheduler backend and the task scheduler.
   * 返回一个二元后台调度程序和任务调度
   * TaskScheduler负责任务的提交,并且请求集群管理器对任务调度,TaskSchedule可以看做任务调度的客户端
   * createTaskScheduler 根据Master的配置匹配部署模式,创建TaskSchedulerImpl,并且生成不同SchedulerBackend
   */
  private def createTaskScheduler(
      sc: SparkContext,
      master: String): (SchedulerBackend, TaskScheduler) = {
    import SparkMasterRegex._

    // When running locally, don't try to re-execute tasks on failure.
    //在本地运行时,不要尝试在失败时重新执行任务。
    val MAX_LOCAL_TASK_FAILURES = 1

    master match {
      case "local" =>
        val scheduler = new TaskSchedulerImpl(sc, MAX_LOCAL_TASK_FAILURES, isLocal = true)
        val backend = new LocalBackend(sc.getConf, scheduler, 1)
        //TaskSchedulerImpl初始化,调度默认FiFO模式
        scheduler.initialize(backend)
        (backend, scheduler)

      case LOCAL_N_REGEX(threads) =>
        //Runtime.getRuntime().availableProcessors()方法获得当前设备的CPU个数
        def localCpuCount: Int = Runtime.getRuntime.availableProcessors()
        // local[*] estimates the number of cores on the machine; local[N] uses exactly N threads.
        //local[*]估计机器上的内核数量,local[N]使用正n个线程
        val threadCount = if (threads == "*") localCpuCount else threads.toInt
        if (threadCount <= 0) {
          throw new SparkException(s"Asked to run locally with $threadCount threads")
        }
        val scheduler = new TaskSchedulerImpl(sc, MAX_LOCAL_TASK_FAILURES, isLocal = true)
        val backend = new LocalBackend(sc.getConf, scheduler, threadCount)
        scheduler.initialize(backend)
        (backend, scheduler)

      case LOCAL_N_FAILURES_REGEX(threads, maxFailures) =>
        //Runtime.getRuntime().availableProcessors()方法获得当前设备的CPU个数
        def localCpuCount: Int = Runtime.getRuntime.availableProcessors()
        // local[*, M] means the number of cores on the computer with M failures
        //local [*，M]表示计算机上有M个故障的内核数
        // local[N, M] means exactly N threads with M failures
        //本地[N,M]意味着具有M个故障的N个线程
        val threadCount = if (threads == "*") localCpuCount else threads.toInt
        val scheduler = new TaskSchedulerImpl(sc, maxFailures.toInt, isLocal = true)
        val backend = new LocalBackend(sc.getConf, scheduler, threadCount)
        scheduler.initialize(backend)
        (backend, scheduler)

      case SPARK_REGEX(sparkUrl) =>
        val scheduler = new TaskSchedulerImpl(sc)
        val masterUrls = sparkUrl.split(",").map("spark://" + _)
        val backend = new SparkDeploySchedulerBackend(scheduler, sc, masterUrls)
        scheduler.initialize(backend)
        (backend, scheduler)
      //memoryPerSlave 指每个Worker占用的内存大小
      //numSlaves 指Worker数量(numSlaves)
      //coresPerSlave 指每个Work占用的CPU核数
      //memoryPerSlave必须大于executorMemory,因为Worker的内存大小包括executor占用的内存
      case LOCAL_CLUSTER_REGEX(numSlaves, coresPerSlave, memoryPerSlave) =>
        // Check to make sure memory requested <= memoryPerSlave. Otherwise Spark will just hang.
        //检查以确保内存请求<= memoryPerSlave,否则Spark会挂起。
        val memoryPerSlaveInt = memoryPerSlave.toInt
        if (sc.executorMemory > memoryPerSlaveInt) {
          throw new SparkException(
            "Asked to launch cluster with %d MB RAM / worker but requested %d MB/worker".format(
              memoryPerSlaveInt, sc.executorMemory))
        }

        val scheduler = new TaskSchedulerImpl(sc)
        
        val localCluster = new LocalSparkCluster(
          numSlaves.toInt, coresPerSlave.toInt, memoryPerSlaveInt, sc.conf)
        //启动集群
        val masterUrls = localCluster.start()
        val backend = new SparkDeploySchedulerBackend(scheduler, sc, masterUrls)
        scheduler.initialize(backend)
        //用于当Drive关闭时关闭集群,仅限于local-cluster
        backend.shutdownCallback = (backend: SparkDeploySchedulerBackend) => {
          localCluster.stop()
        }
        (backend, scheduler)

      case "yarn-standalone" | "yarn-cluster" =>
        if (master == "yarn-standalone") {
          logWarning(
            "\"yarn-standalone\" is deprecated as of Spark 1.0. Use \"yarn-cluster\" instead.")
        }
        val scheduler = try {
          val clazz = Utils.classForName("org.apache.spark.scheduler.cluster.YarnClusterScheduler")
          //getConstructor 获得构造函数,参数类型SparkContext
          val cons = clazz.getConstructor(classOf[SparkContext])
          //asInstanceOf强制类型转换TaskSchedulerImpl对象
          cons.newInstance(sc).asInstanceOf[TaskSchedulerImpl]
        } catch {
          // TODO: Enumerate the exact reasons why it can fail
          // But irrespective of it, it means we cannot proceed !
          //但无论如何,这意味着我们不能继续下去！
          case e: Exception => {
            throw new SparkException("YARN mode not available ?", e)
          }
        }
        val backend = try {
          val clazz =
            Utils.classForName("org.apache.spark.scheduler.cluster.YarnClusterSchedulerBackend")
          val cons = clazz.getConstructor(classOf[TaskSchedulerImpl], classOf[SparkContext])
          cons.newInstance(scheduler, sc).asInstanceOf[CoarseGrainedSchedulerBackend]
        } catch {
          case e: Exception => {
            throw new SparkException("YARN mode not available ?", e)
          }
        }
        scheduler.initialize(backend)
        (backend, scheduler)

      case "yarn-client" =>
        val scheduler = try {
          val clazz = Utils.classForName("org.apache.spark.scheduler.cluster.YarnScheduler")
          val cons = clazz.getConstructor(classOf[SparkContext])
          cons.newInstance(sc).asInstanceOf[TaskSchedulerImpl]

        } catch {
          case e: Exception => {
            throw new SparkException("YARN mode not available ?", e)
          }
        }

        val backend = try {
          val clazz =
            Utils.classForName("org.apache.spark.scheduler.cluster.YarnClientSchedulerBackend")
          val cons = clazz.getConstructor(classOf[TaskSchedulerImpl], classOf[SparkContext])
          cons.newInstance(scheduler, sc).asInstanceOf[CoarseGrainedSchedulerBackend]
        } catch {
          case e: Exception => {
            throw new SparkException("YARN mode not available ?", e)
          }
        }

        scheduler.initialize(backend)
        (backend, scheduler)

      case mesosUrl @ MESOS_REGEX(_) =>
        MesosNativeLibrary.load()
        val scheduler = new TaskSchedulerImpl(sc)
        val coarseGrained = sc.conf.getBoolean("spark.mesos.coarse", false)
        val url = mesosUrl.stripPrefix("mesos://") // strip scheme from raw Mesos URLs
        val backend = if (coarseGrained) {
          new CoarseMesosSchedulerBackend(scheduler, sc, url, sc.env.securityManager)
        } else {
          new MesosSchedulerBackend(scheduler, sc, url)
        }
        scheduler.initialize(backend)
        (backend, scheduler)

      case SIMR_REGEX(simrUrl) =>
        val scheduler = new TaskSchedulerImpl(sc)
        val backend = new SimrSchedulerBackend(scheduler, sc, simrUrl)
        scheduler.initialize(backend)
        (backend, scheduler)

      case _ =>
        throw new SparkException("Could not parse Master URL: '" + master + "'")
    }
  }
}

/**
 * A collection of regexes for extracting information from the master string.
 * 使用正则表达式从Master字符串中提取集合信息
 */
private object SparkMasterRegex {
  // Regular expression used for local[N] and local[*] master formats
  //注意转换\转义|\符匹配,正则表达式用于 local[N] and local[*]
  val LOCAL_N_REGEX = """local\[([0-9]+|\*)\]""".r//
  // Regular expression for local[N, maxRetries], used in tests with failing tasks
  //正则表达式用于local[N, maxRetries],用于测试失败的任务
  val LOCAL_N_FAILURES_REGEX = """local\[([0-9]+|\*)\s*,\s*([0-9]+)\]""".r
  // Regular expression for simulating a Spark cluster of [N, cores, memory] locally
  //正则表达式用于模拟Spark本地集群
  val LOCAL_CLUSTER_REGEX = """local-cluster\[\s*([0-9]+)\s*,\s*([0-9]+)\s*,\s*([0-9]+)\s*]""".r
  // Regular expression for connecting to Spark deploy clusters
  //用于连接到Spark部署集群的正则表达式
  val SPARK_REGEX = """spark://(.*)""".r
  //正则表达式连接到目标群 by mesos:// or zk:// url
  // Regular expression for connection to Mesos cluster by mesos:// or zk:// url
  val MESOS_REGEX = """(mesos|zk)://.*""".r
  // Regular expression for connection to Simr cluster
  // 用于连接到集群环境的正则表达式
  val SIMR_REGEX = """simr://(.*)""".r
}

/**
 * A class encapsulating how to convert some type T to Writable. It stores both the Writable class
 * corresponding to T (e.g. IntWritable for Int) and a function for doing the conversion.
 * The getter for the writable class takes a ClassTag[T] in case this is a generic object
 * that doesn't know the type of T when it is created. This sounds strange but is necessary to
 * support converting subclasses of Writable to themselves (writableWritableConverter).
  *
  * 封装如何将某些类型T转换为可写的类,它存储对应于T的可写类(例如，Int Intrritable for Int)和用于进行转换的功能,
  * 可写类的getter需要一个ClassTag [T],以防这是一个在创建时不知道T的类型的通用对象,
  * 这听起来很奇怪,但是需要支持将Writable的子类转换为自己(writableWritableConverter)
 */
private[spark] class WritableConverter[T](
    val writableClass: ClassTag[T] => Class[_ <: Writable],
    val convert: Writable => T)
  extends Serializable

object WritableConverter {

  // Helper objects for converting common types to Writable
  //帮助对象将常用类型转换为可写
  private[spark] def simpleWritableConverter[T, W <: Writable: ClassTag](convert: W => T)
  : WritableConverter[T] = {
    val wClass = classTag[W].runtimeClass.asInstanceOf[Class[W]]
    new WritableConverter[T](_ => wClass, x => convert(x.asInstanceOf[W]))
  }

  // The following implicit functions were in SparkContext before 1.3 and users had to
  // `import SparkContext._` to enable them. Now we move them here to make the compiler find
  // them automatically. However, we still keep the old functions in SparkContext for backward
  // compatibility and forward to the following functions directly.
  //以下隐含函数在1.3之前的SparkContext中,用户必须“导入SparkContext._”以启用它们,
  // 现在我们将它们移到这里，使编译器自动找到它们,但是,我们仍然保留SparkContext中的旧功能以实现向后兼容,并直接转发到以下功能

  implicit def intWritableConverter(): WritableConverter[Int] =
    simpleWritableConverter[Int, IntWritable](_.get)

  implicit def longWritableConverter(): WritableConverter[Long] =
    simpleWritableConverter[Long, LongWritable](_.get)

  implicit def doubleWritableConverter(): WritableConverter[Double] =
    simpleWritableConverter[Double, DoubleWritable](_.get)

  implicit def floatWritableConverter(): WritableConverter[Float] =
    simpleWritableConverter[Float, FloatWritable](_.get)

  implicit def booleanWritableConverter(): WritableConverter[Boolean] =
    simpleWritableConverter[Boolean, BooleanWritable](_.get)

  implicit def bytesWritableConverter(): WritableConverter[Array[Byte]] = {
    simpleWritableConverter[Array[Byte], BytesWritable] { bw =>
      // getBytes method returns array which is longer then data to be returned
      //getBytes方法返回的数据长于要返回的数据
      Arrays.copyOfRange(bw.getBytes, 0, bw.getLength)
    }
  }

  implicit def stringWritableConverter(): WritableConverter[String] =
    simpleWritableConverter[String, Text](_.toString)

  implicit def writableWritableConverter[T <: Writable](): WritableConverter[T] =
    new WritableConverter[T](_.runtimeClass.asInstanceOf[Class[T]], _.asInstanceOf[T])
}

/**
 * A class encapsulating how to convert some type T to Writable. It stores both the Writable class
 * corresponding to T (e.g. IntWritable for Int) and a function for doing the conversion.
 * The Writable class will be used in `SequenceFileRDDFunctions`.
  * 封装如何将某些类型T转换为可写的类,它存储对应于T的可写类(例如，Int Intrritable for Int)
  * 和用于进行转换的函数,可写类将用于“SequenceFileRDDFunctions”。
 */
private[spark] class WritableFactory[T](
    val writableClass: ClassTag[T] => Class[_ <: Writable],
    val convert: T => Writable) extends Serializable

object WritableFactory {

  private[spark] def simpleWritableFactory[T: ClassTag, W <: Writable : ClassTag](convert: T => W)
    : WritableFactory[T] = {
    val writableClass = implicitly[ClassTag[W]].runtimeClass.asInstanceOf[Class[W]]
    new WritableFactory[T](_ => writableClass, convert)
  }

  implicit def intWritableFactory: WritableFactory[Int] =
    simpleWritableFactory(new IntWritable(_))

  implicit def longWritableFactory: WritableFactory[Long] =
    simpleWritableFactory(new LongWritable(_))

  implicit def floatWritableFactory: WritableFactory[Float] =
    simpleWritableFactory(new FloatWritable(_))

  implicit def doubleWritableFactory: WritableFactory[Double] =
    simpleWritableFactory(new DoubleWritable(_))

  implicit def booleanWritableFactory: WritableFactory[Boolean] =
    simpleWritableFactory(new BooleanWritable(_))

  implicit def bytesWritableFactory: WritableFactory[Array[Byte]] =
    simpleWritableFactory(new BytesWritable(_))

  implicit def stringWritableFactory: WritableFactory[String] =
    simpleWritableFactory(new Text(_))

  implicit def writableWritableFactory[T <: Writable: ClassTag]: WritableFactory[T] =
    simpleWritableFactory(w => w)

}
