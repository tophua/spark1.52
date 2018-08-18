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

package org.apache.spark.deploy.yarn

import java.io.File
import java.net.URI
import java.nio.ByteBuffer

import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.spark.util.Utils

import scala.collection.JavaConversions._
import scala.collection.mutable.{HashMap, ListBuffer}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.NMClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.ipc.YarnRPC
import org.apache.hadoop.yarn.util.{ConverterUtils, Records}

import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkException}
import org.apache.spark.network.util.JavaUtils

class ExecutorRunnable(
    container: Container,
    conf: Configuration,
    sparkConf: SparkConf,
    masterAddress: String,
    slaveId: String,
    hostname: String,
    executorMemory: Int,
    executorCores: Int,
    appId: String,
    securityMgr: SecurityManager)
  extends Runnable with Logging {

  var rpc: YarnRPC = YarnRPC.create(conf)
  var nmClient: NMClient = _
  val yarnConf: YarnConfiguration = new YarnConfiguration(conf)
  lazy val env = prepareEnvironment(container)

  override def run(): Unit = {
    logInfo("Starting Executor Container")
    nmClient = NMClient.createNMClient()
    nmClient.init(yarnConf)
    nmClient.start()
    startContainer()
  }

  def startContainer(): java.util.Map[String, ByteBuffer] = {
    logInfo("Setting up ContainerLaunchContext")

    val ctx = Records.newRecord(classOf[ContainerLaunchContext])
      .asInstanceOf[ContainerLaunchContext]

    val localResources = prepareLocalResources
    ctx.setLocalResources(localResources)

    ctx.setEnvironment(env)

    val credentials = UserGroupInformation.getCurrentUser().getCredentials()
    val dob = new DataOutputBuffer()
    credentials.writeTokenStorageToStream(dob)
    ctx.setTokens(ByteBuffer.wrap(dob.getData()))

    val commands = prepareCommand(masterAddress, slaveId, hostname, executorMemory, executorCores,
      appId, localResources)

    logInfo(s"""
      |===============================================================================
      |YARN executor launch context:
      |  env:
      |${env.map { case (k, v) => s"    $k -> $v\n" }.mkString}
      |  command:
      |    ${commands.mkString(" ")}
      |===============================================================================
      """.stripMargin)

    ctx.setCommands(commands)
    ctx.setApplicationACLs(YarnSparkHadoopUtil.getApplicationAclsForYarn(securityMgr))

    // If external shuffle service is enabled, register with the Yarn shuffle service already
    // started on the NodeManager and, if authentication is enabled, provide it with our secret
    // key for fetching shuffle files later
    //如果启用了外部shuffle服务,请注册已在NodeManager上启动的Yarn shuffle服务,
    //如果启用了身份验证,则为其提供我们的密钥以便稍后获取随机文件
    if (sparkConf.getBoolean("spark.shuffle.service.enabled", false)) {
      val secretString = securityMgr.getSecretKey()
      val secretBytes =
        if (secretString != null) {
          // This conversion must match how the YarnShuffleService decodes our secret
          //此转换必须与YarnShuffleService解密我们的秘密的方式相匹配
          JavaUtils.stringToBytes(secretString)
        } else {
          // Authentication is not enabled, so just provide dummy metadata
          //ByteBuffer.allocate在能够读和写之前,必须有一个缓冲区,用静态方法 allocate() 来分配缓冲区
          ByteBuffer.allocate(0)
        }
      ctx.setServiceData(Map[String, ByteBuffer]("spark_shuffle" -> secretBytes))
    }

    // Send the start request to the ContainerManager
    //将开始请求发送到ContainerManager
    try {
      nmClient.startContainer(container, ctx)
    } catch {
      case ex: Exception =>
        throw new SparkException(s"Exception while starting container ${container.getId}" +
          s" on host $hostname", ex)
    }
  }

  private def prepareCommand(
      masterAddress: String,
      slaveId: String,
      hostname: String,
      executorMemory: Int,
      executorCores: Int,
      appId: String,
      localResources: HashMap[String, LocalResource]): List[String] = {
    // Extra options for the JVM
    //JVM的额外选项
    val javaOpts = ListBuffer[String]()

    // Set the environment variable through a command prefix
    // to append to the existing value of the variable
    //通过命令前缀设置环境变量以附加到变量的现有值
    var prefixEnv: Option[String] = None

    // Set the JVM memory 设置JVM内存
    val executorMemoryString = executorMemory + "m"
    javaOpts += "-Xms" + executorMemoryString
    javaOpts += "-Xmx" + executorMemoryString

    // Set extra Java options for the executor, if defined
    //如果已定义,请为执行程序设置额外的Java选项
    sys.props.get("spark.executor.extraJavaOptions").foreach { opts =>
      javaOpts ++= Utils.splitCommandString(opts).map(YarnSparkHadoopUtil.escapeForShell)
    }
    //System.getenv()和System.getProperties()的区别
    //System.getenv() 返回系统环境变量值 设置系统环境变量：当前登录用户主目录下的".bashrc"文件中可以设置系统环境变量
    //System.getProperties() 返回Java进程变量值 通过命令行参数的"-D"选项
    sys.env.get("SPARK_JAVA_OPTS").foreach { opts =>
      javaOpts ++= Utils.splitCommandString(opts).map(YarnSparkHadoopUtil.escapeForShell)
    }
    sys.props.get("spark.executor.extraLibraryPath").foreach { p =>
      prefixEnv = Some(Client.getClusterPath(sparkConf, Utils.libraryPathEnvPrefix(Seq(p))))
    }

    javaOpts += "-Djava.io.tmpdir=" +
      new Path(
        YarnSparkHadoopUtil.expandEnvironment(Environment.PWD),
        YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR
      )

    // Certain configs need to be passed here because they are needed before the Executor
    // registers with the Scheduler and transfers the spark configs. Since the Executor backend
    // uses Akka to connect to the scheduler, the akka settings are needed as well as the
    // authentication settings.
    //某些配置需要在这里传递,因为在Executor向Scheduler注册并传输spark配置之前需要它们,
    //由于Executor后端使用Akka连接到调度程序,因此需要akka设置以及身份验证设置。
    sparkConf.getAll
      .filter { case (k, v) => SparkConf.isExecutorStartupConf(k) }
      .foreach { case (k, v) => javaOpts += YarnSparkHadoopUtil.escapeForShell(s"-D$k=$v") }

    // Commenting it out for now - so that people can refer to the properties if required. Remove
    // it once cpuset version is pushed out.
    // The context is, default gc for server class machines end up using all cores to do gc - hence
    //暂时评论它 - 以便人们可以在需要时参考这些属性,一旦cpuset版本被推出就删除它,上下文是,
    //服务器类机器的默认gc最终使用所有核心来执行gc - 因此
    // if there are multiple containers in same node, spark gc effects all other containers
    // performance (which can also be other spark containers)
    //如果同一节点中有多个容器,spark gc会影响所有其他容器的性能（也可能是其他spark容器）
    // Instead of using this, rely on cpusets by YARN to enforce spark behaves 'properly' in
    // multi-tenant environments. Not sure how default java gc behaves if it is limited to subset
    // of cores on a node.
    //而不是使用它，依靠YARN的cpusets在多租户环境中“正确”强制执行spark行为,如果java gc仅限于节点上的核心子集,则不确定它的默认行为。
    /*
        else {
          // If no java_opts specified, default to using -XX:+CMSIncrementalMode
          // It might be possible that other modes/config is being done in
          // spark.executor.extraJavaOptions, so we dont want to mess with it.
          // In our expts, using (default) throughput collector has severe perf ramnifications in
          // multi-tennent machines
          // The options are based on
          // http://www.oracle.com/technetwork/java/gc-tuning-5-138395.html#0.0.0.%20When%20to%20Use
          // %20the%20Concurrent%20Low%20Pause%20Collector|outline
          javaOpts += "-XX:+UseConcMarkSweepGC"
          javaOpts += "-XX:+CMSIncrementalMode"
          javaOpts += "-XX:+CMSIncrementalPacing"
          javaOpts += "-XX:CMSIncrementalDutyCycleMin=0"
          javaOpts += "-XX:CMSIncrementalDutyCycle=10"
        }
    */

    // For log4j configuration to reference
    javaOpts += ("-Dspark.yarn.app.container.log.dir=" + ApplicationConstants.LOG_DIR_EXPANSION_VAR)

    val userClassPath = Client.getUserClasspath(sparkConf).flatMap { uri =>
      val absPath =
        if (new File(uri.getPath()).isAbsolute()) {
          Client.getClusterPath(sparkConf, uri.getPath())
        } else {
          Client.buildPath(Environment.PWD.$(), uri.getPath())
        }
      Seq("--user-class-path", "file:" + absPath)
    }.toSeq

    val commands = prefixEnv ++ Seq(
      YarnSparkHadoopUtil.expandEnvironment(Environment.JAVA_HOME) + "/bin/java",
      "-server",
      // Kill if OOM is raised - leverage yarn's failure handling to cause rescheduling.
      //如果OOM被提升则杀死 - 利用yarn的故障处理来重新安排。
      // Not killing the task leaves various aspects of the executor and (to some extent) the jvm in
      // an inconsistent state.
      //不杀死任务会使执行程序的各个方面和(在某种程度上)jvm处于不一致状态
      // TODO: If the OOM is not recoverable by rescheduling it on different node, then do
      // 'something' to fail job ... akin to blacklisting trackers in mapred ?
      "-XX:OnOutOfMemoryError='kill %p'") ++
      javaOpts ++
      Seq("org.apache.spark.executor.CoarseGrainedExecutorBackend",
        "--driver-url", masterAddress.toString,
        "--executor-id", slaveId.toString,
        "--hostname", hostname.toString,
        "--cores", executorCores.toString,
        "--app-id", appId) ++
      userClassPath ++
      Seq(
        "1>", ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout",
        "2>", ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr")

    // TODO: it would be nicer to just make sure there are no null commands here
    commands.map(s => if (s == null) "null" else s).toList
  }

  private def setupDistributedCache(
      file: String,
      rtype: LocalResourceType,
      localResources: HashMap[String, LocalResource],
      timestamp: String,
      size: String,
      vis: String): Unit = {
    val uri = new URI(file)
    val amJarRsrc = Records.newRecord(classOf[LocalResource])
    amJarRsrc.setType(rtype)
    amJarRsrc.setVisibility(LocalResourceVisibility.valueOf(vis))
    amJarRsrc.setResource(ConverterUtils.getYarnUrlFromURI(uri))
    amJarRsrc.setTimestamp(timestamp.toLong)
    amJarRsrc.setSize(size.toLong)
    localResources(uri.getFragment()) = amJarRsrc
  }

  private def prepareLocalResources: HashMap[String, LocalResource] = {
    logInfo("Preparing Local resources")
    val localResources = HashMap[String, LocalResource]()
    //System.getenv()和System.getProperties()的区别
    //System.getenv() 返回系统环境变量值 设置系统环境变量：当前登录用户主目录下的".bashrc"文件中可以设置系统环境变量
    //System.getProperties() 返回Java进程变量值 通过命令行参数的"-D"选项
    if (System.getenv("SPARK_YARN_CACHE_FILES") != null) {
      val timeStamps = System.getenv("SPARK_YARN_CACHE_FILES_TIME_STAMPS").split(',')
      val fileSizes = System.getenv("SPARK_YARN_CACHE_FILES_FILE_SIZES").split(',')
      val distFiles = System.getenv("SPARK_YARN_CACHE_FILES").split(',')
      val visibilities = System.getenv("SPARK_YARN_CACHE_FILES_VISIBILITIES").split(',')
      for( i <- 0 to distFiles.length - 1) {
        setupDistributedCache(distFiles(i), LocalResourceType.FILE, localResources, timeStamps(i),
          fileSizes(i), visibilities(i))
      }
    }
    //System.getenv()和System.getProperties()的区别
    //System.getenv() 返回系统环境变量值 设置系统环境变量：当前登录用户主目录下的".bashrc"文件中可以设置系统环境变量
    //System.getProperties() 返回Java进程变量值 通过命令行参数的"-D"选项
    if (System.getenv("SPARK_YARN_CACHE_ARCHIVES") != null) {
      val timeStamps = System.getenv("SPARK_YARN_CACHE_ARCHIVES_TIME_STAMPS").split(',')
      val fileSizes = System.getenv("SPARK_YARN_CACHE_ARCHIVES_FILE_SIZES").split(',')
      val distArchives = System.getenv("SPARK_YARN_CACHE_ARCHIVES").split(',')
      val visibilities = System.getenv("SPARK_YARN_CACHE_ARCHIVES_VISIBILITIES").split(',')
      for( i <- 0 to distArchives.length - 1) {
        setupDistributedCache(distArchives(i), LocalResourceType.ARCHIVE, localResources,
          timeStamps(i), fileSizes(i), visibilities(i))
      }
    }

    logInfo("Prepared Local resources " + localResources)
    localResources
  }

  private def prepareEnvironment(container: Container): HashMap[String, String] = {
    val env = new HashMap[String, String]()
    val extraCp = sparkConf.getOption("spark.executor.extraClassPath")
    Client.populateClasspath(null, yarnConf, sparkConf, env, false, extraCp)

    sparkConf.getExecutorEnv.foreach { case (key, value) =>
      // This assumes each executor environment variable set here is a path
      // This is kept for backward compatibility and consistency with hadoop
      YarnSparkHadoopUtil.addPathToEnvironment(env, key, value)
    }
    //System.getenv()和System.getProperties()的区别
    //System.getenv() 返回系统环境变量值 设置系统环境变量：当前登录用户主目录下的".bashrc"文件中可以设置系统环境变量
    //System.getProperties() 返回Java进程变量值 通过命令行参数的"-D"选项
    // Keep this for backwards compatibility but users should move to the config
    sys.env.get("SPARK_YARN_USER_ENV").foreach { userEnvs =>
      YarnSparkHadoopUtil.setEnvFromInputString(env, userEnvs)
    }

    // lookup appropriate http scheme for container log urls
    val yarnHttpPolicy = yarnConf.get(
      YarnConfiguration.YARN_HTTP_POLICY_KEY,
      YarnConfiguration.YARN_HTTP_POLICY_DEFAULT
    )
    val httpScheme = if (yarnHttpPolicy == "HTTPS_ONLY") "https://" else "http://"

    // Add log urls
    //System.getenv()和System.getProperties()的区别
    //System.getenv() 返回系统环境变量值 设置系统环境变量：当前登录用户主目录下的".bashrc"文件中可以设置系统环境变量
    //System.getProperties() 返回Java进程变量值 通过命令行参数的"-D"选项
    sys.env.get("SPARK_USER").foreach { user =>
      val containerId = ConverterUtils.toString(container.getId)
      val address = container.getNodeHttpAddress
      val baseUrl = s"$httpScheme$address/node/containerlogs/$containerId/$user"

      env("SPARK_LOG_URL_STDERR") = s"$baseUrl/stderr?start=-4096"
      env("SPARK_LOG_URL_STDOUT") = s"$baseUrl/stdout?start=-4096"
    }

    System.getenv().filterKeys(_.startsWith("SPARK")).foreach { case (k, v) => env(k) = v }
    env
  }
}
