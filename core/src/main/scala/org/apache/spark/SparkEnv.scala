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
import java.net.Socket

import akka.actor.ActorSystem

import scala.collection.mutable
import scala.util.Properties

import com.google.common.collect.MapMaker

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.python.PythonWorkerFactory
import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.network.BlockTransferService
import org.apache.spark.network.netty.NettyBlockTransferService
import org.apache.spark.network.nio.NioBlockTransferService
import org.apache.spark.rpc.{ RpcEndpointRef, RpcEndpoint, RpcEnv }
import org.apache.spark.rpc.akka.AkkaRpcEnv
import org.apache.spark.scheduler.{ OutputCommitCoordinator, LiveListenerBus }
import org.apache.spark.scheduler.OutputCommitCoordinator.OutputCommitCoordinatorEndpoint
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{ ShuffleMemoryManager, ShuffleManager }
import org.apache.spark.storage._
import org.apache.spark.unsafe.memory.{ ExecutorMemoryManager, MemoryAllocator }
import org.apache.spark.util.{ RpcUtils, Utils }

/**
 * :: DeveloperApi ::
 * Holds all the runtime environment objects for a running Spark instance (either master or worker),
 * including the serializer, Akka actor system, block manager, map output tracker, etc. Currently
 * Spark code finds the SparkEnv through a global variable, so all the threads can access the same
 * SparkEnv. It can be accessed by SparkEnv.get (e.g. after creating a SparkContext).
 *
 * NOTE: This is not intended for external use. This is exposed for Shark and may be made private
 *       in a future release.
 * SparkEnv是Spark的执行环境对象,其中包括众多与Executor执行相关的对象,由于在local模式下Driver模式下Driver会创建Executor
 * local-cluster部署模式或者Standalone部署模式下Worker另起的CoarseGrainedExecutorBackend进程中也会创建Executor,
 * 所以SparkEnv存在于Driver或CoarseGrainedExecutorBackend进程中,创建SparkEnv主要使用SparkEnv的CreateDriver
 *
 */
@DeveloperApi
class SparkEnv(
    val executorId: String,
    private[spark] val rpcEnv: RpcEnv,
    val serializer: Serializer,//类序列化
    val closureSerializer: Serializer, //闭包序列化
    val cacheManager: CacheManager, //用于存储中间计算结果
    /**
     * 保存Shuffle Map Task输出的位置信息,其中在Driver上的Tracer是MapOutputTrackerMaster
     * 而在Executor上的Tracker是MapOutputTrackerWorker,它会从MapOutputTrackerMaster获取信息
     */
    val mapOutputTracker: MapOutputTracker, //用来缓存MapStatus信息，并提供从MapOutputMaster获取信息的功能
    /**
     * Shuffle管理者,其中Driver端会注册Shuffle的信息,而Executor会上报和获取Shuffle信息
     * 现阶段内部支持Hash base Shuffle和 Sort Based Shuffle
     */
    val shuffleManager: ShuffleManager,//Shuffle元数据管理,例如:shuffleId,MapTask的数量
    val broadcastManager: BroadcastManager, //广播变量管理者
    val blockTransferService: BlockTransferService, //Executor读取Shuffle数据的Client
    val blockManager: BlockManager, //块管理,提供Storage模块与其他模块的交互接口,管理Storage模块
    val securityManager: SecurityManager, //Spark对于认证授权的实现
    val httpFileServer: HttpFileServer, //主要用于Executor端下载依赖
    val sparkFilesDir: String, //文件存储目录
    val metricsSystem: MetricsSystem, //用于搜集统计信息
    val shuffleMemoryManager: ShuffleMemoryManager, //负责管理Shuffle线程占有内存的分配与释放
    //负责管理Executor线程占有内存的分配与释放,当Task退出时这个内存也会被回收
    val executorMemoryManager: ExecutorMemoryManager, 
    val outputCommitCoordinator: OutputCommitCoordinator,
    val conf: SparkConf //配置文件 
    ) extends Logging {

  // TODO Remove actorSystem
  //actorSystem是Akka提供用于创建分布式消息通信系统的基础类
  @deprecated("Actor system is no longer supported as of 1.4.0", "1.4.0")
  val actorSystem: ActorSystem = rpcEnv.asInstanceOf[AkkaRpcEnv].actorSystem

  private[spark] var isStopped = false
  private val pythonWorkers = mutable.HashMap[(String, Map[String, String]), PythonWorkerFactory]()

  // A general, soft-reference map for metadata needed during HadoopRDD split computation
  // (e.g., HadoopFileRDD uses this to cache JobConfs and InputFormats).
  private[spark] val hadoopJobMetadata = new MapMaker().softValues().makeMap[String, Any]()

  private var driverTmpDirToDelete: Option[String] = None

  private[spark] def stop() {

    if (!isStopped) {
      isStopped = true
      pythonWorkers.values.foreach(_.stop())
      Option(httpFileServer).foreach(_.stop())
      mapOutputTracker.stop()
      shuffleManager.stop()
      broadcastManager.stop()
      blockManager.stop()
      blockManager.master.stop()
      metricsSystem.stop()
      outputCommitCoordinator.stop()
      rpcEnv.shutdown()

      // Unfortunately Akka's awaitTermination doesn't actually wait for the Netty server to shut
      // down, but let's call it anyway in case it gets fixed in a later release
      // UPDATE: In Akka 2.1.x, this hangs if there are remote actors, so we can't call it.
      // actorSystem.awaitTermination()

      // Note that blockTransferService is stopped by BlockManager since it is started by it.

      // If we only stop sc, but the driver process still run as a services then we need to delete
      // the tmp dir, if not, it will create too many tmp dirs.
      // We only need to delete the tmp dir create by driver, because sparkFilesDir is point to the
      // current working dir in executor which we do not need to delete.
      driverTmpDirToDelete match {
        case Some(path) => { //如果有path值
          try {
            Utils.deleteRecursively(new File(path))
          } catch {
            case e: Exception =>
              logWarning(s"Exception while deleting Spark temp dir: $path", e)
          }
        }
        //没有值
        case None => // We just need to delete tmp dir created by driver, so do nothing on executor
      }
    }
  }

  private[spark] def createPythonWorker(pythonExec: String, envVars: Map[String, String]): java.net.Socket = {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.getOrElseUpdate(key, new PythonWorkerFactory(pythonExec, envVars)).create()
    }
  }

  private[spark] def destroyPythonWorker(pythonExec: String, envVars: Map[String, String], worker: Socket) {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.get(key).foreach(_.stopWorker(worker))
    }
  }

  private[spark] def releasePythonWorker(pythonExec: String, envVars: Map[String, String], worker: Socket) {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.get(key).foreach(_.releaseWorker(worker))
    }
  }
}

object SparkEnv extends Logging {
  @volatile private var env: SparkEnv = _

  private[spark] val driverActorSystemName = "sparkDriver"
  private[spark] val executorActorSystemName = "sparkExecutor"

  def set(e: SparkEnv) {
    env = e
  }

  /**
   * Returns the SparkEnv.
   */
  def get: SparkEnv = {
    env
  }

  /**
   * Returns the ThreadLocal SparkEnv.
   */
  @deprecated("Use SparkEnv.get instead", "1.2.0")
  def getThreadLocal: SparkEnv = {
    env
  }

  /**
   * Create a SparkEnv for the driver.
   * conf是对SparkConf复制
   * isLocal 是否单机模式
   * listenerBus 采用监听器模式维护各类事件的处理
   */
  private[spark] def createDriverEnv(
    conf: SparkConf,
    isLocal: Boolean,
    listenerBus: LiveListenerBus,
    numCores: Int,
    mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {
    assert(conf.contains("spark.driver.host"), "spark.driver.host is not set on the driver!")
    assert(conf.contains("spark.driver.port"), "spark.driver.port is not set on the driver!")
    //驱动器监听主机名或者IP地址.
    val hostname = conf.get("spark.driver.host")
    //驱动器监听端口号
    val port = conf.get("spark.driver.port").toInt
    create(
      conf,
      SparkContext.DRIVER_IDENTIFIER,
      hostname,
      port,
      isDriver = true,
      isLocal = isLocal,
      numUsableCores = numCores,
      listenerBus = listenerBus,
      mockOutputCommitCoordinator = mockOutputCommitCoordinator)
  }

  /**
   * Create a SparkEnv for an executor.
   * In coarse-grained mode, the executor provides an actor system that is already instantiated.
   */
  private[spark] def createExecutorEnv(
    conf: SparkConf,
    executorId: String,
    hostname: String,
    port: Int,
    numCores: Int,
    isLocal: Boolean): SparkEnv = {
    val env = create(
      conf,
      executorId,
      hostname,
      port,
      isDriver = false,
      isLocal = isLocal,
      numUsableCores = numCores)
    SparkEnv.set(env)
    env
  }

  /**
   * Helper method to create a SparkEnv for a driver or an executor.
   * SparkEnv创建步骤如下:
   * 1)创建安全管理器SecurityManager
   * 2)创建基于Akka的分布式消息系统ActorSystem
   * 3)创建Map任务输出跟踪器mapOutTracker
   * 4)实例化ShuffleManager
   * 5)创建ShuffleMemoryManager
   * 6)创建块传输服务BlockTransferService
   * 7)创建BlockManagerMaster
   * 8)创建块管理器BlockManager
   * 9)创建广播管理器BroadcasterManager
   * 10)创建缓存管理器CacheManager
   * 11)创建Http文件服务器HttpFileServicer
   * 12)创建测量系统MetricesSystem
   * 13)创建SparkEnv
   */
  private def create(
    conf: SparkConf,
    executorId: String,
    hostname: String, //机器
    port: Int, //端口
    isDriver: Boolean,
    isLocal: Boolean, //是否单机模式
    numUsableCores: Int, //可使用内核数
    listenerBus: LiveListenerBus = null, //采用监听器模式维护各类事件处理
    mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {

    // Listener bus is only used on the driver
    if (isDriver) {
      assert(listenerBus != null, "Attempted to create driver SparkEnv with null listener bus!")
    }
    //创建安全管理器
    val securityManager = new SecurityManager(conf)
    //创建基本Akka的分布式消息系统ActorSystem
    //ActorSystem是Akka提供的用于创建分布式消息通信系统的基础
    // Create the ActorSystem for Akka and get the port it binds to.
    val actorSystemName = if (isDriver) driverActorSystemName else executorActorSystemName
    val rpcEnv = RpcEnv.create(actorSystemName, hostname, port, conf, securityManager)
    val actorSystem = rpcEnv.asInstanceOf[AkkaRpcEnv].actorSystem

    // Figure out which port Akka actually bound to in case the original port is 0 or occupied.
    if (isDriver) {
      conf.set("spark.driver.port", rpcEnv.address.port.toString)
    } else {
      conf.set("spark.executor.port", rpcEnv.address.port.toString)
    }

    // Create an instance of the class with the given name, possibly initializing it with our conf

    def instantiateClass[T](className: String): T = {
      val cls = Utils.classForName(className)
      // Look for a constructor taking a SparkConf and a boolean isDriver, then one taking just
      // SparkConf, then one taking no arguments
      try {
        cls.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
          .newInstance(conf, new java.lang.Boolean(isDriver))
          .asInstanceOf[T]
      } catch {
        case _: NoSuchMethodException =>
          try {
            cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[T]
          } catch {
            case _: NoSuchMethodException =>
              cls.getConstructor().newInstance().asInstanceOf[T]
          }
      }
    }

    // Create an instance of the class named by the given SparkConf property, or defaultClassName
    // if the property is not set, possibly initializing it with our conf
    def instantiateClassFromConf[T](propertyName: String, defaultClassName: String): T = {
      instantiateClass[T](conf.get(propertyName, defaultClassName))
    }

    val serializer = instantiateClassFromConf[Serializer](
      "spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    logDebug(s"Using serializer: ${serializer.getClass}")
    //闭包序列化类
    val closureSerializer = instantiateClassFromConf[Serializer](
      "spark.closure.serializer", "org.apache.spark.serializer.JavaSerializer")
    //用于查找或者注册Actor的实现
    def registerOrLookupEndpoint(
      name: String, endpointCreator: => RpcEndpoint): RpcEndpointRef = {
      if (isDriver) {
        //如果当前节点是Driver则创建这个Actor
        logInfo("Registering " + name)
        rpcEnv.setupEndpoint(name, endpointCreator)
      } else {
        //否则建立到Driver的连接,取得BlockManagerMaster的Actor
        RpcUtils.makeDriverRef(name, conf, rpcEnv)
      }
    }
    //map任务输出跟踪器,用于跟踪map阶段任务的输出状态,此状态便于reduce阶段任务获取地址及中间输出结果
    //每个Map任务或者Reduce任务都会有其唯一标识,分别为mapId和reduceId,每个reduce任务的输入可能是多个Map任务的输出,
    //reduce会到各个Map任务的所在节点上拉取Block,这个过程叫做Shuffle,每批过程都有唯一ShuufleId
    val mapOutputTracker = if (isDriver) {
      //如果当前应用程序是Driver,则创建MapOutputTrackerMasterActor,注册到ActorSystem
      new MapOutputTrackerMaster(conf)
    } else {
      //如果当前是Exceutor,则创建MapOutputTrackerWorker,并从ActorSystem中找到MapOutputTrackerMasterActor
      new MapOutputTrackerWorker(conf)
    }

    // Have to assign trackerActor after initialization as MapOutputTrackerActor
    // requires the MapOutputTracker itself
    //将MapOutputTracker注册ActorSystem,mapOutputTracker的属性trackerEndpoint持有MapOutputTrackerMasterEndpoint的引用
    /**
     * 查找或者注册Actor
     * Map任务的状态正是由Executor向持有的MapOutputTrackerMasterActor发送消息,将Map任务状态同步到MapOutPutTracker的MapStatus
     * 和cachedSerializedStatuses,
     * Executor如何找到MapOutputTrackerMasterActor?registerOrLookupEndpoint方法通过调用AkkaUtils.makeDriverRef找到MapOutputTrackerMasterActor
     */
    mapOutputTracker.trackerEndpoint = registerOrLookupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(
        rpcEnv, mapOutputTracker.asInstanceOf[MapOutputTrackerMaster], conf))

    // Let the user specify short names for shuffle managers
    //shuffleManager 负责管理本地及远程的block数据的shuffle操作
    val shortShuffleMgrNames = Map(
      "hash" -> "org.apache.spark.shuffle.hash.HashShuffleManager",
      "sort" -> "org.apache.spark.shuffle.sort.SortShuffleManager",
      "tungsten-sort" -> "org.apache.spark.shuffle.unsafe.UnsafeShuffleManager")
    //默认使用SortShuffleManager实例,通过持有的IndexShuffleBlockManager间接操作BlockManager中的DiskBlockManager
    //将Map结果写入本地,并根据shuffleId,MapId写入索引文件,也能通过MapOutputTrackerMaster中维护的MapStatuses从本地
    //或者其他远程节点读取文件
    val shuffleMgrName = conf.get("spark.shuffle.manager", "sort") //默认SortShuffleManager
    val shuffleMgrClass = shortShuffleMgrNames.getOrElse(shuffleMgrName.toLowerCase, shuffleMgrName)
    //实例ShuffleManager,通过反射方式生成实例
    val shuffleManager = instantiateClass[ShuffleManager](shuffleMgrClass)
    //shuffleMemoryManager负责管理shuffle线程占有内存的分配与释放
    val shuffleMemoryManager = ShuffleMemoryManager.create(conf, numUsableCores)
    //创建块传输服务
    val blockTransferService =
      conf.get("spark.shuffle.blockTransferService", "netty").toLowerCase match {
        case "netty" =>
          //netty提供的异步事件驱动的网络应用框架,提供web服务及客户端,获取远程节点上Block的集合
          new NettyBlockTransferService(conf, securityManager, numUsableCores)
        case "nio" =>
          logWarning("NIO-based block transfer service is deprecated, " +
            "and will be removed in Spark 1.6.0.")//不赞成使用,1.6删除
          new NioBlockTransferService(conf, securityManager)
      }
    //创建blockManagerMaster负责对Block的管理和协调,具体操作于BlockManagerMasterActor
    /**
     * Driver和Executor处理BlockManagerMaster的方式不同
     * 1)如果当前应用程序是Driver,则创建BlockManagerMasterActor,并且注册到ActorSystem中
     * 2)如果当前应用程序是Executor,则从ActorSystem中找到BlockManagerMasterActor
     * 无论是Driver还是Executor,最后BlockManagerMaster的属性driverActor将持有对BlockManagerMasterActor的引用
     */
    val blockManagerMaster = new BlockManagerMaster(registerOrLookupEndpoint(
      BlockManagerMaster.DRIVER_ENDPOINT_NAME,
      new BlockManagerMasterEndpoint(rpcEnv, isLocal, conf, listenerBus)),
      conf, isDriver)

    // NB: blockManager is not valid until initialize() is called later.
    //创建块管理器BlockManager,负责对Block的管理,只有在BlockManager的初始化方法initialize调用后,它才有效.    
    val blockManager = new BlockManager(executorId, rpcEnv, blockManagerMaster,
      serializer, conf, mapOutputTracker, shuffleManager, blockTransferService, securityManager,
      numUsableCores)
    //创建广播管理器BroadcastManager,用于将配置信息和序列化后的RDD,Job以及ShuffleDependency等信息在本地存储
    //如果为了容灾,也会复制到其他节点上
    val broadcastManager = new BroadcastManager(isDriver, conf, securityManager)
    //创建缓存管理器CacheManager,用于缓存RDD某个分区计算后的中间结果,缓存计算结果发生在迭代计算的时候
    val cacheManager = new CacheManager(blockManager)
    //创建HTTP文件服务器httpFileServer
    val httpFileServer =
      if (isDriver) {
        //主要提供对jar及其他文件的http访问,这些jar包括用户上传的jar包
        //spark.fileserver.port,0默认表示随机生成
        val fileServerPort = conf.getInt("spark.fileserver.port", 0)
        val server = new HttpFileServer(conf, securityManager, fileServerPort)
        server.initialize()
        conf.set("spark.fileserver.uri", server.serverUri)
        server
      } else {
        null
      }
    //创建测量系统
    val metricsSystem = if (isDriver) {
      // Don't start metrics system right now for Driver.
      // We need to wait for the task scheduler to give us an app ID.
      // Then we can start the metrics system.
      MetricsSystem.createMetricsSystem("driver", conf, securityManager)
    } else {
      // We need to set the executor ID before the MetricsSystem is created because sources and
      // sinks specified in the metrics configuration file will want to incorporate this executor's
      // ID into the metrics they report.
      conf.set("spark.executor.id", executorId)
      val ms = MetricsSystem.createMetricsSystem("executor", conf, securityManager)
      ms.start()
      ms
    }

    // Set the sparkFiles directory, used when downloading dependencies.  In local mode,
    // this is a temporary directory; in distributed mode, this is the executor's current working
    // directory.
    val sparkFilesDir: String = if (isDriver) {
      Utils.createTempDir(Utils.getLocalDir(conf), "userFiles").getAbsolutePath
    } else {
      "."
    }

    val outputCommitCoordinator = mockOutputCommitCoordinator.getOrElse {
      new OutputCommitCoordinator(conf, isDriver)
    }
    val outputCommitCoordinatorRef = registerOrLookupEndpoint("OutputCommitCoordinator",
      new OutputCommitCoordinatorEndpoint(rpcEnv, outputCommitCoordinator))
    outputCommitCoordinator.coordinatorRef = Some(outputCommitCoordinatorRef)
  //负责管理Executor线程占有内存的分配与释放,当Task退出时这个内存也会被回收
  //Spark Tungsten project 引入的新的内存管理机制
    val executorMemoryManager: ExecutorMemoryManager = {
      val allocator = if (conf.getBoolean("spark.unsafe.offHeap", false)) {
        MemoryAllocator.UNSAFE //org.apache.spark.unsafe.memory.UnsafeMemoryAllocator
      } else {
        MemoryAllocator.HEAP//org.apache.spark.unsafe.memory.HeapMemoryAllocator
      }
      new ExecutorMemoryManager(allocator)
    }
    //创建SparkEnv
    val envInstance = new SparkEnv(
      executorId,
      rpcEnv,
      serializer,
      closureSerializer,
      cacheManager,
      mapOutputTracker,
      shuffleManager,
      broadcastManager,
      blockTransferService,
      blockManager,
      securityManager,
      httpFileServer,
      sparkFilesDir,
      metricsSystem,
      shuffleMemoryManager,
      executorMemoryManager,
      outputCommitCoordinator,
      conf)

    // Add a reference to tmp dir created by driver, we will delete this tmp dir when stop() is
    // called, and we only need to do it for driver. Because driver may run as a service, and if we
    // don't delete this tmp dir when sc is stopped, then will create too many tmp dirs.
    if (isDriver) {
      envInstance.driverTmpDirToDelete = Some(sparkFilesDir)
    }

    envInstance
  }

  /**
   * Return a map representation of jvm information, Spark properties, system properties, and
   * class paths. Map keys define the category, and map values represent the corresponding
   * attributes as a sequence of KV pairs. This is used mainly for SparkListenerEnvironmentUpdate.
   */
  private[spark] def environmentDetails(
    conf: SparkConf,
    schedulingMode: String,
    addedJars: Seq[String],
    addedFiles: Seq[String]): Map[String, Seq[(String, String)]] = {

    import Properties._
    val jvmInformation = Seq(
      ("Java Version", s"$javaVersion ($javaVendor)"),
      ("Java Home", javaHome),
      ("Scala Version", versionString)).sorted

    // Spark properties
    // This includes the scheduling mode whether or not it is configured (used by SparkUI)
    //Spark的任务调度模式
    val schedulerMode =
      if (!conf.contains("spark.scheduler.mode")) {
        Seq(("spark.scheduler.mode", schedulingMode))
      } else {
        Seq[(String, String)]()
      }
    val sparkProperties = (conf.getAll ++ schedulerMode).sorted

    // System properties that are not java classpaths
    val systemProperties = Utils.getSystemProperties.toSeq
    val otherProperties = systemProperties.filter {
      case (k, _) =>
        k != "java.class.path" && !k.startsWith("spark.")
    }.sorted

    // Class paths including all added jars and files
    val classPathEntries = javaClassPath
      .split(File.pathSeparator)
      .filterNot(_.isEmpty)
      .map((_, "System Classpath"))
    val addedJarsAndFiles = (addedJars ++ addedFiles).map((_, "Added By User"))
    val classPaths = (addedJarsAndFiles ++ classPathEntries).sorted

    Map[String, Seq[(String, String)]](
      "JVM Information" -> jvmInformation,
      "Spark Properties" -> sparkProperties,
      "System Properties" -> otherProperties,
      "Classpath Entries" -> classPaths)
  }
}
