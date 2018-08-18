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

import java.io.{ByteArrayInputStream, DataInputStream, File, FileOutputStream, IOException,
  OutputStreamWriter}
import java.net.{InetAddress, UnknownHostException, URI, URISyntaxException}
import java.nio.ByteBuffer
import java.security.PrivilegedExceptionAction
import java.util.{Properties, UUID}
import java.util.zip.{ZipEntry, ZipOutputStream}

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, ListBuffer, Map}
import scala.reflect.runtime.universe
import scala.util.{Try, Success, Failure}
import scala.util.control.NonFatal

import com.google.common.base.Charsets.UTF_8
import com.google.common.base.Objects
import com.google.common.io.Files

import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.MRJobConfig
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.{TokenIdentifier, Token}
import org.apache.hadoop.util.StringUtils
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.{YarnClient, YarnClientApplication}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException
import org.apache.hadoop.yarn.util.Records

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkContext, SparkException}
import org.apache.spark.util.Utils

private[spark] class Client(
    val args: ClientArguments,
    val hadoopConf: Configuration,
    val sparkConf: SparkConf)
  extends Logging {

  import Client._

  def this(clientArgs: ClientArguments, spConf: SparkConf) =
    this(clientArgs, SparkHadoopUtil.get.newConfiguration(spConf), spConf)

  def this(clientArgs: ClientArguments) = this(clientArgs, new SparkConf())

  private val yarnClient = YarnClient.createYarnClient
  private val yarnConf = new YarnConfiguration(hadoopConf)
  private var credentials: Credentials = null
  private val amMemoryOverhead = args.amMemoryOverhead // MB
  private val executorMemoryOverhead = args.executorMemoryOverhead // MB
  private val distCacheMgr = new ClientDistributedCacheManager()
  private val isClusterMode = args.isClusterMode

  private var loginFromKeytab = false
  private var principal: String = null
  private var keytab: String = null

  private val fireAndForget = isClusterMode &&
    !sparkConf.getBoolean("spark.yarn.submit.waitAppCompletion", true)

  def stop(): Unit = {
    yarnClient.stop()
    // Unset YARN mode system env variable, to allow switching between cluster types.
    //取消设置YARN模式系统env变量，以允许在群集类型之间切换
    System.clearProperty("SPARK_YARN_MODE")
  }

  /**
   * Submit an application running our ApplicationMaster to the ResourceManager.
    * 将运行ApplicationMaster的应用程序提交到ResourceManager
   *
   * The stable Yarn API provides a convenience method (YarnClient#createApplication) for
   * creating applications and setting up the application submission context. This was not
   * available in the alpha API.
    * 稳定的Yarn API提供了一种方便的方法(YarnClient＃createApplication),
    * 用于创建应用程序和设置应用程序提交上下文,这在alpha API中不可用。
   */
  def submitApplication(): ApplicationId = {
    var appId: ApplicationId = null
    try {
      // Setup the credentials before doing anything else,
      // so we have don't have issues at any point.
      //在做任何其他事情之前设置凭证,所以我们在任何时候都没有问题
      setupCredentials()
      yarnClient.init(yarnConf)
      yarnClient.start()

      logInfo("Requesting a new application from cluster with %d NodeManagers"
        .format(yarnClient.getYarnClusterMetrics.getNumNodeManagers))

      // Get a new application from our RM
      //从我们的RM获取新的应用程序
      val newApp = yarnClient.createApplication()
      val newAppResponse = newApp.getNewApplicationResponse()
      appId = newAppResponse.getApplicationId()

      // Verify whether the cluster has enough resources for our AM
      //验证群集是否有足够的资源用于AM
      verifyClusterResources(newAppResponse)

      // Set up the appropriate contexts to launch our AM
      //设置适当的上下文以启动我们的AM
      val containerContext = createContainerLaunchContext(newAppResponse)
      val appContext = createApplicationSubmissionContext(newApp, containerContext)

      // Finally, submit and monitor the application
      //最后,提交并监控申请
      logInfo(s"Submitting application ${appId.getId} to ResourceManager")
      yarnClient.submitApplication(appContext)
      appId
    } catch {
      case e: Throwable =>
        if (appId != null) {
          cleanupStagingDir(appId)
        }
        throw e
    }
  }

  /**
   * Cleanup application staging directory.
    * 清理应用程序临时目录
   */
  private def cleanupStagingDir(appId: ApplicationId): Unit = {
    val appStagingDir = getAppStagingDir(appId)
    try {
      val preserveFiles = sparkConf.getBoolean("spark.yarn.preserve.staging.files", false)
      val stagingDirPath = new Path(appStagingDir)
      val fs = FileSystem.get(hadoopConf)
      if (!preserveFiles && fs.exists(stagingDirPath)) {
        logInfo("Deleting staging directory " + stagingDirPath)
        fs.delete(stagingDirPath, true)
      }
    } catch {
      case ioe: IOException =>
        logWarning("Failed to cleanup staging dir " + appStagingDir, ioe)
    }
  }

  /**
   * Set up the context for submitting our ApplicationMaster.
    * 设置提交ApplicationMaster的上下文
   * This uses the YarnClientApplication not available in the Yarn alpha API.
    * 这使用了Yarn alpha API中没有的YarnClientApplication
   */
  def createApplicationSubmissionContext(
      newApp: YarnClientApplication,
      containerContext: ContainerLaunchContext): ApplicationSubmissionContext = {
    val appContext = newApp.getApplicationSubmissionContext
    appContext.setApplicationName(args.appName)
    appContext.setQueue(args.amQueue)
    appContext.setAMContainerSpec(containerContext)
    appContext.setApplicationType("SPARK")
    sparkConf.getOption("spark.yarn.maxAppAttempts").map(_.toInt) match {
      case Some(v) => appContext.setMaxAppAttempts(v)
      case None => logDebug("spark.yarn.maxAppAttempts is not set. " +
          "Cluster's default value will be used.")
    }
    val capability = Records.newRecord(classOf[Resource])
    capability.setMemory(args.amMemory + amMemoryOverhead)
    capability.setVirtualCores(args.amCores)
    appContext.setResource(capability)
    appContext
  }

  /** Set up security tokens for launching our ApplicationMaster container.
    * 设置安全令牌以启动我们的ApplicationMaster容器*/
  private def setupSecurityToken(amContainer: ContainerLaunchContext): Unit = {
    val dob = new DataOutputBuffer
    credentials.writeTokenStorageToStream(dob)
    amContainer.setTokens(ByteBuffer.wrap(dob.getData))
  }

  /** Get the application report from the ResourceManager for an application we have submitted.
    * 从ResourceManager获取我们提交的应用程序的应用程序报告*/
  def getApplicationReport(appId: ApplicationId): ApplicationReport =
    yarnClient.getApplicationReport(appId)

  /**
   * Return the security token used by this client to communicate with the ApplicationMaster.
    * 返回此客户端使用的安全令牌以与ApplicationMaster通信
   * If no security is enabled, the token returned by the report is null.
    * 如果未启用安全性,则报告返回的标记为空
   */
  private def getClientToken(report: ApplicationReport): String =
    Option(report.getClientToAMToken).map(_.toString).getOrElse("")

  /**
   * Fail fast if we have requested more resources per container than is available in the cluster.
    * 如果我们请求每个容器的资源多于群集中可用的资源,则会快速失败。
   */
  private def verifyClusterResources(newAppResponse: GetNewApplicationResponse): Unit = {
    val maxMem = newAppResponse.getMaximumResourceCapability().getMemory()
    logInfo("Verifying our application has not requested more than the maximum " +
      s"memory capability of the cluster ($maxMem MB per container)")
    val executorMem = args.executorMemory + executorMemoryOverhead
    if (executorMem > maxMem) {
      throw new IllegalArgumentException(s"Required executor memory (${args.executorMemory}" +
        s"+$executorMemoryOverhead MB) is above the max threshold ($maxMem MB) of this cluster! " +
        "Please increase the value of 'yarn.scheduler.maximum-allocation-mb'.")
    }
    val amMem = args.amMemory + amMemoryOverhead
    if (amMem > maxMem) {
      throw new IllegalArgumentException(s"Required AM memory (${args.amMemory}" +
        s"+$amMemoryOverhead MB) is above the max threshold ($maxMem MB) of this cluster! " +
        "Please increase the value of 'yarn.scheduler.maximum-allocation-mb'.")
    }
    logInfo("Will allocate AM container, with %d MB memory including %d MB overhead".format(
      amMem,
      amMemoryOverhead))

    // We could add checks to make sure the entire cluster has enough resources but that involves
    // getting all the node reports and computing ourselves.
  }

  /**
   * Copy the given file to a remote file system (e.g. HDFS) if needed.
    * 如果需要,将给定文件复制到远程文件系统(例如HDFS)
   * The file is only copied if the source and destination file systems are different. This is used
   * for preparing resources for launching the ApplicationMaster container. Exposed for testing.
    * 仅在源和目标文件系统不同时才复制该文件,这用于准备启动ApplicationMaster容器的资源,
    * 暴露在测试中。
   */
  private[yarn] def copyFileToRemote(
      destDir: Path,
      srcPath: Path,
      replication: Short): Path = {
    val destFs = destDir.getFileSystem(hadoopConf)
    val srcFs = srcPath.getFileSystem(hadoopConf)
    var destPath = srcPath
    if (!compareFs(srcFs, destFs)) {
      destPath = new Path(destDir, srcPath.getName())
      logInfo(s"Uploading resource $srcPath -> $destPath")
      FileUtil.copy(srcFs, srcPath, destFs, destPath, false, hadoopConf)
      destFs.setReplication(destPath, replication)
      destFs.setPermission(destPath, new FsPermission(APP_FILE_PERMISSION))
    } else {
      logInfo(s"Source and destination file systems are the same. Not copying $srcPath")
    }
    // Resolve any symlinks in the URI path so using a "current" symlink to point to a specific
    // version shows the specific version in the distributed cache configuration
    //解析URI路径中的所有符号链接,以便使用“当前”符号链接指向特定版本,以显示分布式缓存配置中的特定版本
    val qualifiedDestPath = destFs.makeQualified(destPath)
    val fc = FileContext.getFileContext(qualifiedDestPath.toUri(), hadoopConf)
    fc.resolvePath(qualifiedDestPath)
  }

  /**
   * Upload any resources to the distributed cache if needed. If a resource is intended to be
   * consumed locally, set up the appropriate config for downstream code to handle it properly.
    * 如果需要,将任何资源上载到分布式缓存,如果要在本地使用资源,请为下游代码设置适当的配置以正确处理它。
   * This is used for setting up a container launch context for our ApplicationMaster.
    * 这用于为ApplicationMaster设置容器启动上下文
   * Exposed for testing.
   */
  def prepareLocalResources(
      appStagingDir: String,
      pySparkArchives: Seq[String]): HashMap[String, LocalResource] = {
    logInfo("Preparing resources for our AM container")
    // Upload Spark and the application JAR to the remote file system if necessary,
    // and add them as local resources to the application master.
    val fs = FileSystem.get(hadoopConf)
    val dst = new Path(fs.getHomeDirectory(), appStagingDir)
    val nns = YarnSparkHadoopUtil.get.getNameNodesToAccess(sparkConf) + dst
    YarnSparkHadoopUtil.get.obtainTokensForNamenodes(nns, hadoopConf, credentials)
    // Used to keep track of URIs added to the distributed cache. If the same URI is added
    // multiple times, YARN will fail to launch containers for the app with an internal
    // error.
    val distributedUris = new HashSet[String]
    obtainTokenForHiveMetastore(sparkConf, hadoopConf, credentials)
    obtainTokenForHBase(hadoopConf, credentials)

    val replication = sparkConf.getInt("spark.yarn.submit.file.replication",
      fs.getDefaultReplication(dst)).toShort
    val localResources = HashMap[String, LocalResource]()
    FileSystem.mkdirs(fs, dst, new FsPermission(STAGING_DIR_PERMISSION))

    val statCache: Map[URI, FileStatus] = HashMap[URI, FileStatus]()

    val oldLog4jConf = Option(System.getenv("SPARK_LOG4J_CONF"))
    if (oldLog4jConf.isDefined) {
      logWarning(
        "SPARK_LOG4J_CONF detected in the system environment. This variable has been " +
          "deprecated. Please refer to the \"Launching Spark on YARN\" documentation " +
          "for alternatives.")
    }

    def addDistributedUri(uri: URI): Boolean = {
      val uriStr = uri.toString()
      if (distributedUris.contains(uriStr)) {
        logWarning(s"Resource $uri added multiple times to distributed cache.")
        false
      } else {
        distributedUris += uriStr
        true
      }
    }

    /**
     * Distribute a file to the cluster.
     * 将文件分发到群集
     * If the file's path is a "local:" URI, it's actually not distributed. Other files are copied
     * to HDFS (if not already there) and added to the application's distributed cache.
     * 如果文件的路径是“local：”URI，则它实际上不是分布式的,
      *其他文件被复制到HDFS（如果尚未存在）并添加到应用程序的分布式缓存中。
     * @param path URI of the file to distribute. 要分发的文件的URI
     * @param resType Type of resource being distributed.正在分发的资源类型
     * @param destName Name of the file in the distributed cache.分布式缓存中文件的名称
     * @param targetDir Subdirectory where to place the file.子目录放置文件的位置
     * @param appMasterOnly Whether to distribute only to the AM.是否仅分发给AM
     * @return A 2-tuple. First item is whether the file is a "local:" URI. Second item is the
     *         localized path for non-local paths, or the input `path` for local paths.
     *         The localized path will be null if the URI has already been added to the cache.
     */
    def distribute(
        path: String,
        resType: LocalResourceType = LocalResourceType.FILE,
        destName: Option[String] = None,
        targetDir: Option[String] = None,
        appMasterOnly: Boolean = false): (Boolean, String) = {
      val trimmedPath = path.trim()
      val localURI = Utils.resolveURI(trimmedPath)
      if (localURI.getScheme != LOCAL_SCHEME) {
        if (addDistributedUri(localURI)) {
          val localPath = getQualifiedLocalPath(localURI, hadoopConf)
          val linkname = targetDir.map(_ + "/").getOrElse("") +
            destName.orElse(Option(localURI.getFragment())).getOrElse(localPath.getName())
          val destPath = copyFileToRemote(dst, localPath, replication)
          val destFs = FileSystem.get(destPath.toUri(), hadoopConf)
          distCacheMgr.addResource(
            destFs, hadoopConf, destPath, localResources, resType, linkname, statCache,
            appMasterOnly = appMasterOnly)
          (false, linkname)
        } else {
          (false, null)
        }
      } else {
        (true, trimmedPath)
      }
    }

    // If we passed in a keytab, make sure we copy the keytab to the staging directory on
    // HDFS, and setup the relevant environment vars, so the AM can login again.
    //如果我们传入密钥表,请确保将密钥表复制到HDFS上的登台目录,并设置相关的环境变量,以便AM可以再次登录
    if (loginFromKeytab) {
      logInfo("To enable the AM to login from keytab, credentials are being copied over to the AM" +
        " via the YARN Secure Distributed Cache.")
      val (_, localizedPath) = distribute(keytab,
        destName = Some(sparkConf.get("spark.yarn.keytab")),
        appMasterOnly = true)
      require(localizedPath != null, "Keytab file already distributed.")
    }

    /**
     * Copy the given main resource to the distributed cache if the scheme is not "local".
     * Otherwise, set the corresponding key in our SparkConf to handle it downstream.
      * 如果方案不是“本地”，则将给定的主资源复制到分布式缓存,
      * 否则,在我们的SparkConf中设置相应的键以在下游处理它,
     * Each resource is represented by a 3-tuple of:
     *   (1) destination resource name,
     *   (2) local path to the resource,
     *   (3) Spark property key to set if the scheme is not local
     */
    List(
      (SPARK_JAR, sparkJar(sparkConf), CONF_SPARK_JAR),
      (APP_JAR, args.userJar, CONF_SPARK_USER_JAR),
      ("log4j.properties", oldLog4jConf.orNull, null)
    ).foreach { case (destName, path, confKey) =>
      if (path != null && !path.trim().isEmpty()) {
        val (isLocal, localizedPath) = distribute(path, destName = Some(destName))
        if (isLocal && confKey != null) {
          require(localizedPath != null, s"Path $path already distributed.")
          // If the resource is intended for local use only, handle this downstream
          // by setting the appropriate property
          sparkConf.set(confKey, localizedPath)
        }
      }
    }

    /**
     * Do the same for any additional resources passed in through ClientArguments.
      * 对通过ClientArguments传入的任何其他资源执行相同操作
     * Each resource category is represented by a 3-tuple of:
      * 每个资源类别由3元组表示：
     *   (1) comma separated list of resources in this category,
      *      逗号分隔此类别中的资源列表
     *   (2) resource type, and
     *   (3) whether to add these resources to the classpath
     */
    val cachedSecondaryJarLinks = ListBuffer.empty[String]
    List(
      (args.addJars, LocalResourceType.FILE, true),
      (args.files, LocalResourceType.FILE, false),
      (args.archives, LocalResourceType.ARCHIVE, false)
    ).foreach { case (flist, resType, addToClasspath) =>
      if (flist != null && !flist.isEmpty()) {
        flist.split(',').foreach { file =>
          val (_, localizedPath) = distribute(file, resType = resType)
          require(localizedPath != null)
          if (addToClasspath) {
            cachedSecondaryJarLinks += localizedPath
          }
        }
      }
    }
    if (cachedSecondaryJarLinks.nonEmpty) {
      sparkConf.set(CONF_SPARK_YARN_SECONDARY_JARS, cachedSecondaryJarLinks.mkString(","))
    }

    if (isClusterMode && args.primaryPyFile != null) {
      distribute(args.primaryPyFile, appMasterOnly = true)
    }

    pySparkArchives.foreach { f => distribute(f) }

    // The python files list needs to be treated especially. All files that are not an
    // archive need to be placed in a subdirectory that will be added to PYTHONPATH.
    //需要特别处理python文件列表,所有非归档文件都需要放在将添加到PYTHONPATH的子目录中
    args.pyFiles.foreach { f =>
      val targetDir = if (f.endsWith(".py")) Some(LOCALIZED_PYTHON_DIR) else None
      distribute(f, targetDir = targetDir)
    }

    // Distribute an archive with Hadoop and Spark configuration for the AM.
    //为AM分发带有Hadoop和Spark配置的存档
    val (_, confLocalizedPath) = distribute(createConfArchive().toURI().getPath(),
      resType = LocalResourceType.ARCHIVE,
      destName = Some(LOCALIZED_CONF_DIR),
      appMasterOnly = true)
    require(confLocalizedPath != null)

    localResources
  }

  /**
   * Create an archive with the config files for distribution.
   * 使用配置文件创建存档以进行分发
   * These are only used by the AM, since executors will use the configuration object broadcast by
   * the driver. The files are zipped and added to the job as an archive, so that YARN will explode
   * it when distributing to the AM. This directory is then added to the classpath of the AM
   * process, just to make sure that everybody is using the same default config.
    * 这些仅由AM使用,因为执行程序将使用驱动程序广播的配置对象,压缩文件并将其作为存档添加到作业中,
    * 以便YARN在分发到AM时将其爆炸,然后将此目录添加到AM进程的类路径中,以确保每个人都使用相同的默认配置。
   *
   * This follows the order of precedence set by the startup scripts, in which HADOOP_CONF_DIR
   * shows up in the classpath before YARN_CONF_DIR.
    * 这遵循启动脚本设置的优先顺序,其中HADOOP_CONF_DIR在YARN_CONF_DIR之前的类路径中显示
   *
   * Currently this makes a shallow copy of the conf directory. If there are cases where a
   * Hadoop config directory contains subdirectories, this code will have to be fixed.
    *
   * 目前,这是conf目录的浅表副本,如果存在Hadoop配置目录包含子目录的情况,则必须修复此代码。
    *
   * The archive also contains some Spark configuration. Namely, it saves the contents of
   * SparkConf in a file to be loaded by the AM process.
    *
    * 存档还包含一些Spark配置,即它将SparkConf的内容保存在要由AM进程加载的文件中
   */
  private def createConfArchive(): File = {
    val hadoopConfFiles = new HashMap[String, File]()
    Seq("HADOOP_CONF_DIR", "YARN_CONF_DIR").foreach { envKey =>
      //System.getenv()和System.getProperties()的区别
      //System.getenv() 返回系统环境变量值 设置系统环境变量：当前登录用户主目录下的".bashrc"文件中可以设置系统环境变量
      //System.getProperties() 返回Java进程变量值 通过命令行参数的"-D"选项
      sys.env.get(envKey).foreach { path =>
        val dir = new File(path)
        if (dir.isDirectory()) {
          dir.listFiles().foreach { file =>
            if (file.isFile && !hadoopConfFiles.contains(file.getName())) {
              hadoopConfFiles(file.getName()) = file
            }
          }
        }
      }
    }

    val confArchive = File.createTempFile(LOCALIZED_CONF_DIR, ".zip",
      new File(Utils.getLocalDir(sparkConf)))
    val confStream = new ZipOutputStream(new FileOutputStream(confArchive))

    try {
      confStream.setLevel(0)
      hadoopConfFiles.foreach { case (name, file) =>
        if (file.canRead()) {
          confStream.putNextEntry(new ZipEntry(name))
          Files.copy(file, confStream)
          confStream.closeEntry()
        }
      }

      // Save Spark configuration to a file in the archive.
      //将Spark配置保存到存档中的文件
      val props = new Properties()
      sparkConf.getAll.foreach { case (k, v) => props.setProperty(k, v) }
      confStream.putNextEntry(new ZipEntry(SPARK_CONF_FILE))
      val writer = new OutputStreamWriter(confStream, UTF_8)
      props.store(writer, "Spark configuration.")
      writer.flush()
      confStream.closeEntry()
    } finally {
      confStream.close()
    }
    confArchive
  }

  /**
   * Get the renewal interval for tokens.
    * 获取令牌的续订间隔
   */
  private def getTokenRenewalInterval(stagingDirPath: Path): Long = {
    // We cannot use the tokens generated above since those have renewer yarn. Trying to renew
    // those will fail with an access control issue. So create new tokens with the logged in
    // user as renewer.
    ///我们不能使用上面生成的令牌,因为那些具有更新yarn,
    // 试图更新它们会因访问控制问题而失败,因此,使用登录用户创建新令牌作为续订者
    val creds = new Credentials()
    val nns = YarnSparkHadoopUtil.get.getNameNodesToAccess(sparkConf) + stagingDirPath
    YarnSparkHadoopUtil.get.obtainTokensForNamenodes(
      nns, hadoopConf, creds, Some(sparkConf.get("spark.yarn.principal")))
    val t = creds.getAllTokens
      .filter(_.getKind == DelegationTokenIdentifier.HDFS_DELEGATION_KIND)
      .head
    val newExpiration = t.renew(hadoopConf)
    val identifier = new DelegationTokenIdentifier()
    identifier.readFields(new DataInputStream(new ByteArrayInputStream(t.getIdentifier)))
    val interval = newExpiration - identifier.getIssueDate
    logInfo(s"Renewal Interval set to $interval")
    interval
  }

  /**
   * Set up the environment for launching our ApplicationMaster container.
    * 设置启动ApplicationMaster容器的环境
   */
  private def setupLaunchEnv(
      stagingDir: String,
      pySparkArchives: Seq[String]): HashMap[String, String] = {
    logInfo("Setting up the launch environment for our AM container")
    val env = new HashMap[String, String]()
    val extraCp = sparkConf.getOption("spark.driver.extraClassPath")
    populateClasspath(args, yarnConf, sparkConf, env, true, extraCp)
    env("SPARK_YARN_MODE") = "true"
    env("SPARK_YARN_STAGING_DIR") = stagingDir
    env("SPARK_USER") = UserGroupInformation.getCurrentUser().getShortUserName()
    if (loginFromKeytab) {
      val remoteFs = FileSystem.get(hadoopConf)
      val stagingDirPath = new Path(remoteFs.getHomeDirectory, stagingDir)
      val credentialsFile = "credentials-" + UUID.randomUUID().toString
      sparkConf.set(
        "spark.yarn.credentials.file", new Path(stagingDirPath, credentialsFile).toString)
      logInfo(s"Credentials file set to: $credentialsFile")
      val renewalInterval = getTokenRenewalInterval(stagingDirPath)
      sparkConf.set("spark.yarn.token.renewal.interval", renewalInterval.toString)
    }

    // Pick up any environment variables for the AM provided through spark.yarn.appMasterEnv.*
    //选择通过spark.yarn.appMasterEnv.*提供的AM的任何环境变量
    val amEnvPrefix = "spark.yarn.appMasterEnv."
    sparkConf.getAll
      .filter { case (k, v) => k.startsWith(amEnvPrefix) }
      .map { case (k, v) => (k.substring(amEnvPrefix.length), v) }
      .foreach { case (k, v) => YarnSparkHadoopUtil.addPathToEnvironment(env, k, v) }

    // Keep this for backwards compatibility but users should move to the config
    //保持此向前兼容性,但用户应移至配置
    //System.getenv()和System.getProperties()的区别
    //System.getenv() 返回系统环境变量值 设置系统环境变量：当前登录用户主目录下的".bashrc"文件中可以设置系统环境变量
    //System.getProperties() 返回Java进程变量值 通过命令行参数的"-D"选项
    sys.env.get("SPARK_YARN_USER_ENV").foreach { userEnvs =>
    // Allow users to specify some environment variables.
      //允许用户指定一些环境变量
      YarnSparkHadoopUtil.setEnvFromInputString(env, userEnvs)
      // Pass SPARK_YARN_USER_ENV itself to the AM so it can use it to set up executor environments.
      //将SPARK_YARN_USER_ENV本身传递给AM,以便它可以使用它来设置执行程序环境
      env("SPARK_YARN_USER_ENV") = userEnvs
    }

    // If pyFiles contains any .py files, we need to add LOCALIZED_PYTHON_DIR to the PYTHONPATH
    // of the container processes too. Add all non-.py files directly to PYTHONPATH.
    //如果pyFiles包含任何.py文件,我们还需要将LOCALIZED_PYTHON_DIR添加到容器进程的PYTHONPATH中
    //将所有non.py文件直接添加到PYTHONPATH。
    // NOTE: the code currently does not handle .py files defined with a "local:" scheme.
    val pythonPath = new ListBuffer[String]()
    val (pyFiles, pyArchives) = args.pyFiles.partition(_.endsWith(".py"))
    if (pyFiles.nonEmpty) {
      pythonPath += buildPath(YarnSparkHadoopUtil.expandEnvironment(Environment.PWD),
        LOCALIZED_PYTHON_DIR)
    }
    (pySparkArchives ++ pyArchives).foreach { path =>
      val uri = Utils.resolveURI(path)
      if (uri.getScheme != LOCAL_SCHEME) {
        pythonPath += buildPath(YarnSparkHadoopUtil.expandEnvironment(Environment.PWD),
          new Path(uri).getName())
      } else {
        pythonPath += uri.getPath()
      }
    }

    // Finally, update the Spark config to propagate PYTHONPATH to the AM and executors.
    //System.getenv()和System.getProperties()的区别
    //System.getenv() 返回系统环境变量值 设置系统环境变量：当前登录用户主目录下的".bashrc"文件中可以设置系统环境变量
    //System.getProperties() 返回Java进程变量值 通过命令行参数的"-D"选项
    if (pythonPath.nonEmpty) {
      val pythonPathStr = (sys.env.get("PYTHONPATH") ++ pythonPath)
        .mkString(YarnSparkHadoopUtil.getClassPathSeparator)
      env("PYTHONPATH") = pythonPathStr
      sparkConf.setExecutorEnv("PYTHONPATH", pythonPathStr)
    }

    // In cluster mode, if the deprecated SPARK_JAVA_OPTS is set, we need to propagate it to
    // executors. But we can't just set spark.executor.extraJavaOptions, because the driver's
    // SparkContext will not let that set spark* system properties, which is expected behavior for
    // Yarn clients. So propagate it through the environment.
    //在集群模式下,如果设置了不推荐的SPARK_JAVA_OPTS,我们需要将其传播给执行程序,
    //但是我们不能只设置spark.executor.extraJavaOptions,因为驱动程序的SparkContext不会让它设置spark*系统属性,
    //这是Yarn客户端的预期行为,所以在环境中传播它。
    // Note that to warn the user about the deprecation in cluster mode, some code from
    // SparkConf#validateSettings() is duplicated here (to avoid triggering the condition
    // described above).
    //System.getenv()和System.getProperties()的区别
    //System.getenv() 返回系统环境变量值 设置系统环境变量：当前登录用户主目录下的".bashrc"文件中可以设置系统环境变量
    //System.getProperties() 返回Java进程变量值 通过命令行参数的"-D"选项
    if (isClusterMode) {
      sys.env.get("SPARK_JAVA_OPTS").foreach { value =>
        val warning =
          s"""
            |SPARK_JAVA_OPTS was detected (set to '$value').
            |This is deprecated in Spark 1.0+.
            |
            |Please instead use:
            | - ./spark-submit with conf/spark-defaults.conf to set defaults for an application
            | - ./spark-submit with --driver-java-options to set -X options for a driver
            | - spark.executor.extraJavaOptions to set -X options for executors
          """.stripMargin
        logWarning(warning)
        for (proc <- Seq("driver", "executor")) {
          val key = s"spark.$proc.extraJavaOptions"
          if (sparkConf.contains(key)) {
            throw new SparkException(s"Found both $key and SPARK_JAVA_OPTS. Use only the former.")
          }
        }
        env("SPARK_JAVA_OPTS") = value
      }
    }
    //System.getenv()和System.getProperties()的区别
    //System.getenv() 返回系统环境变量值 设置系统环境变量：当前登录用户主目录下的".bashrc"文件中可以设置系统环境变量
    //System.getProperties() 返回Java进程变量值 通过命令行参数的"-D"选项
    sys.env.get(ENV_DIST_CLASSPATH).foreach { dcp =>
      env(ENV_DIST_CLASSPATH) = dcp
    }

    env
  }

  /**
   * Set up a ContainerLaunchContext to launch our ApplicationMaster container.
    * 设置ContainerLaunchContext以启动我们的ApplicationMaster容器
   * This sets up the launch environment, java options, and the command for launching the AM.
    * 这将设置启动环境,java选项以及启动AM的命令
   */
  private def createContainerLaunchContext(newAppResponse: GetNewApplicationResponse)
    : ContainerLaunchContext = {
    logInfo("Setting up container launch context for our AM")
    val appId = newAppResponse.getApplicationId
    val appStagingDir = getAppStagingDir(appId)
    val pySparkArchives =
      if (sparkConf.getBoolean("spark.yarn.isPython", false)) {
        findPySparkArchives()
      } else {
        //Nil是一个空的List,::向队列的头部追加数据,创造新的列表
        Nil
      }
    val launchEnv = setupLaunchEnv(appStagingDir, pySparkArchives)
    val localResources = prepareLocalResources(appStagingDir, pySparkArchives)

    // Set the environment variables to be passed on to the executors.
    //设置要传递给执行程序的环境变量
    distCacheMgr.setDistFilesEnv(launchEnv)
    distCacheMgr.setDistArchivesEnv(launchEnv)

    val amContainer = Records.newRecord(classOf[ContainerLaunchContext])
    amContainer.setLocalResources(localResources)
    amContainer.setEnvironment(launchEnv)

    val javaOpts = ListBuffer[String]()

    // Set the environment variable through a command prefix
    // to append to the existing value of the variable
    //通过命令前缀设置环境变量以附加到变量的现有值
    var prefixEnv: Option[String] = None

    // Add Xmx for AM memory
    javaOpts += "-Xmx" + args.amMemory + "m"

    val tmpDir = new Path(
      YarnSparkHadoopUtil.expandEnvironment(Environment.PWD),
      YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR
    )
    javaOpts += "-Djava.io.tmpdir=" + tmpDir

    // TODO: Remove once cpuset version is pushed out.
    // The context is, default gc for server class machines ends up using all cores to do gc -
    // hence if there are multiple containers in same node, Spark GC affects all other containers'
    // performance (which can be that of other Spark containers)
    // Instead of using this, rely on cpusets by YARN to enforce "proper" Spark behavior in
    // multi-tenant environments. Not sure how default Java GC behaves if it is limited to subset
    // of cores on a node.
    val useConcurrentAndIncrementalGC = launchEnv.get("SPARK_USE_CONC_INCR_GC").exists(_.toBoolean)
    if (useConcurrentAndIncrementalGC) {
      // In our expts, using (default) throughput collector has severe perf ramifications in
      // multi-tenant machines
      javaOpts += "-XX:+UseConcMarkSweepGC"
      javaOpts += "-XX:MaxTenuringThreshold=31"
      javaOpts += "-XX:SurvivorRatio=8"
      javaOpts += "-XX:+CMSIncrementalMode"
      javaOpts += "-XX:+CMSIncrementalPacing"
      javaOpts += "-XX:CMSIncrementalDutyCycleMin=0"
      javaOpts += "-XX:CMSIncrementalDutyCycle=10"
    }

    // Include driver-specific java options if we are launching a driver
    //如果我们要启动驱动程序,请包含特定于驱动程序的java选项
    if (isClusterMode) {
      //System.getenv()和System.getProperties()的区别
      //System.getenv() 返回系统环境变量值 设置系统环境变量：当前登录用户主目录下的".bashrc"文件中可以设置系统环境变量
      //System.getProperties() 返回Java进程变量值 通过命令行参数的"-D"选项
      val driverOpts = sparkConf.getOption("spark.driver.extraJavaOptions")
        .orElse(sys.env.get("SPARK_JAVA_OPTS"))
      driverOpts.foreach { opts =>
        javaOpts ++= Utils.splitCommandString(opts).map(YarnSparkHadoopUtil.escapeForShell)
      }
      val libraryPaths = Seq(sys.props.get("spark.driver.extraLibraryPath"),
        sys.props.get("spark.driver.libraryPath")).flatten
      if (libraryPaths.nonEmpty) {
        prefixEnv = Some(getClusterPath(sparkConf, Utils.libraryPathEnvPrefix(libraryPaths)))
      }
      if (sparkConf.getOption("spark.yarn.am.extraJavaOptions").isDefined) {
        logWarning("spark.yarn.am.extraJavaOptions will not take effect in cluster mode")
      }
    } else {
      // Validate and include yarn am specific java options in yarn-client mode.
      //在yarn-client模式下验证并包含yarn am特定的java选项
      val amOptsKey = "spark.yarn.am.extraJavaOptions"
      val amOpts = sparkConf.getOption(amOptsKey)
      amOpts.foreach { opts =>
        if (opts.contains("-Dspark")) {
          val msg = s"$amOptsKey is not allowed to set Spark options (was '$opts'). "
          throw new SparkException(msg)
        }
        if (opts.contains("-Xmx") || opts.contains("-Xms")) {
          val msg = s"$amOptsKey is not allowed to alter memory settings (was '$opts')."
          throw new SparkException(msg)
        }
        javaOpts ++= Utils.splitCommandString(opts).map(YarnSparkHadoopUtil.escapeForShell)
      }

      sparkConf.getOption("spark.yarn.am.extraLibraryPath").foreach { paths =>
        prefixEnv = Some(getClusterPath(sparkConf, Utils.libraryPathEnvPrefix(Seq(paths))))
      }
    }

    // For log4j configuration to reference
    //对于log4j配置来引用
    javaOpts += ("-Dspark.yarn.app.container.log.dir=" + ApplicationConstants.LOG_DIR_EXPANSION_VAR)

    val userClass =
      if (isClusterMode) {
        Seq("--class", YarnSparkHadoopUtil.escapeForShell(args.userClass))
      } else {
        //Nil是一个空的List,::向队列的头部追加数据,创造新的列表
        Nil
      }
    val userJar =
      if (args.userJar != null) {
        Seq("--jar", args.userJar)
      } else {
        Nil
      }
    val primaryPyFile =
      if (isClusterMode && args.primaryPyFile != null) {
        Seq("--primary-py-file", new Path(args.primaryPyFile).getName())
      } else {
        //Nil是一个空的List,::向队列的头部追加数据,创造新的列表
        Nil
      }
    val primaryRFile =
      if (args.primaryRFile != null) {
        Seq("--primary-r-file", args.primaryRFile)
      } else {
        Nil
      }
    val amClass =
      if (isClusterMode) {
        Utils.classForName("org.apache.spark.deploy.yarn.ApplicationMaster").getName
      } else {
        Utils.classForName("org.apache.spark.deploy.yarn.ExecutorLauncher").getName
      }
    if (args.primaryRFile != null && args.primaryRFile.endsWith(".R")) {
      args.userArgs = ArrayBuffer(args.primaryRFile) ++ args.userArgs
    }
    val userArgs = args.userArgs.flatMap { arg =>
      Seq("--arg", YarnSparkHadoopUtil.escapeForShell(arg))
    }
    val amArgs =
      Seq(amClass) ++ userClass ++ userJar ++ primaryPyFile ++ primaryRFile ++
        userArgs ++ Seq(
          "--executor-memory", args.executorMemory.toString + "m",
          "--executor-cores", args.executorCores.toString,
          "--properties-file", buildPath(YarnSparkHadoopUtil.expandEnvironment(Environment.PWD),
            LOCALIZED_CONF_DIR, SPARK_CONF_FILE))

    // Command for the ApplicationMaster
    val commands = prefixEnv ++ Seq(
        YarnSparkHadoopUtil.expandEnvironment(Environment.JAVA_HOME) + "/bin/java", "-server"
      ) ++
      javaOpts ++ amArgs ++
      Seq(
        "1>", ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout",
        "2>", ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr")

    // TODO: it would be nicer to just make sure there are no null commands here
    val printableCommands = commands.map(s => if (s == null) "null" else s).toList
    amContainer.setCommands(printableCommands)

    logDebug("===============================================================================")
    logDebug("YARN AM launch context:")
    logDebug(s"    user class: ${Option(args.userClass).getOrElse("N/A")}")
    logDebug("    env:")
    launchEnv.foreach { case (k, v) => logDebug(s"        $k -> $v") }
    logDebug("    resources:")
    localResources.foreach { case (k, v) => logDebug(s"        $k -> $v")}
    logDebug("    command:")
    logDebug(s"        ${printableCommands.mkString(" ")}")
    logDebug("===============================================================================")

    // send the acl settings into YARN to control who has access via YARN interfaces
    //将acl设置发送到YARN以控制谁可以通过YARN接口进行访问
    val securityManager = new SecurityManager(sparkConf)
    amContainer.setApplicationACLs(YarnSparkHadoopUtil.getApplicationAclsForYarn(securityManager))
    setupSecurityToken(amContainer)
    UserGroupInformation.getCurrentUser().addCredentials(credentials)

    amContainer
  }

  def setupCredentials(): Unit = {
    loginFromKeytab = args.principal != null || sparkConf.contains("spark.yarn.principal")
    if (loginFromKeytab) {
      principal =
        if (args.principal != null) args.principal else sparkConf.get("spark.yarn.principal")
      keytab = {
        if (args.keytab != null) {
          args.keytab
        } else {
          sparkConf.getOption("spark.yarn.keytab").orNull
        }
      }

      require(keytab != null, "Keytab must be specified when principal is specified.")
      logInfo("Attempting to login to the Kerberos" +
        s" using principal: $principal and keytab: $keytab")
      val f = new File(keytab)
      // Generate a file name that can be used for the keytab file, that does not conflict
      // with any user file.
      //生成可用于keytab文件的文件名,该文件名不与任何用户文件冲突
      val keytabFileName = f.getName + "-" + UUID.randomUUID().toString
      sparkConf.set("spark.yarn.keytab", keytabFileName)
      sparkConf.set("spark.yarn.principal", principal)
    }
    credentials = UserGroupInformation.getCurrentUser.getCredentials
  }

  /**
   * Report the state of an application until it has exited, either successfully or
   * due to some failure, then return a pair of the yarn application state (FINISHED, FAILED,
   * KILLED, or RUNNING) and the final application state (UNDEFINED, SUCCEEDED, FAILED,
   * or KILLED).
    * 报告一个应用程序的状态,直到它成功退出,或者由于某些失败,然后返回一对yarn应用状态(完成、失败、死亡或运行)
    * 和最终的应用状态(未定义、成功、失败或被杀死)
   *
   * @param appId ID of the application to monitor. 要监视的应用程序的ID
   * @param returnOnRunning Whether to also return the application state when it is RUNNING.
    *                        是否也在运行时返回应用程序状态
   * @param logApplicationReport Whether to log details of the application report every iteration.
    *                             是否每次迭代都记录应用程序报告的详细信息
   * @return A pair of the yarn application state and the final application state.
    *         一对Yarn的使用状态和最终的应用状态
   */
  def monitorApplication(
      appId: ApplicationId,
      returnOnRunning: Boolean = false,
      logApplicationReport: Boolean = true): (YarnApplicationState, FinalApplicationStatus) = {
    val interval = sparkConf.getLong("spark.yarn.report.interval", 1000)
    var lastState: YarnApplicationState = null
    while (true) {
      Thread.sleep(interval)
      val report: ApplicationReport =
        try {
          getApplicationReport(appId)
        } catch {
          case e: ApplicationNotFoundException =>
            logError(s"Application $appId not found.")
            return (YarnApplicationState.KILLED, FinalApplicationStatus.KILLED)
          case NonFatal(e) =>
            logError(s"Failed to contact YARN for application $appId.", e)
            return (YarnApplicationState.FAILED, FinalApplicationStatus.FAILED)
        }
      val state = report.getYarnApplicationState

      if (logApplicationReport) {
        logInfo(s"Application report for $appId (state: $state)")

        // If DEBUG is enabled, log report details every iteration
        // Otherwise, log them every time the application changes state
        //如果启用了DEBUG,则每次迭代都会记录报告详细信息
        //否则,每次应用程序更改状态时都记录它们
        if (log.isDebugEnabled) {
          logDebug(formatReportDetails(report))
        } else if (lastState != state) {
          logInfo(formatReportDetails(report))
        }
      }

      if (state == YarnApplicationState.FINISHED ||
        state == YarnApplicationState.FAILED ||
        state == YarnApplicationState.KILLED) {
        cleanupStagingDir(appId)
        return (state, report.getFinalApplicationStatus)
      }

      if (returnOnRunning && state == YarnApplicationState.RUNNING) {
        return (state, report.getFinalApplicationStatus)
      }

      lastState = state
    }

    // Never reached, but keeps compiler happy
    throw new SparkException("While loop is depleted! This should never happen...")
  }

  private def formatReportDetails(report: ApplicationReport): String = {
    val details = Seq[(String, String)](
      ("client token", getClientToken(report)),
      ("diagnostics", report.getDiagnostics),
      ("ApplicationMaster host", report.getHost),
      ("ApplicationMaster RPC port", report.getRpcPort.toString),
      ("queue", report.getQueue),
      ("start time", report.getStartTime.toString),
      ("final status", report.getFinalApplicationStatus.toString),
      ("tracking URL", report.getTrackingUrl),
      ("user", report.getUser)
    )

    // Use more loggable format if value is null or empty
    //如果value为null或为空,则使用更多可记录格式
    details.map { case (k, v) =>
      val newValue = Option(v).filter(_.nonEmpty).getOrElse("N/A")
      s"\n\t $k: $newValue"
    }.mkString("")
  }

  /**
   * Submit an application to the ResourceManager.
    * 将应用程序提交到ResourceManager
   * If set spark.yarn.submit.waitAppCompletion to true, it will stay alive
   * reporting the application's status until the application has exited for any reason.
   * Otherwise, the client process will exit after submission.
   * If the application finishes with a failed, killed, or undefined status,
   * throw an appropriate SparkException.
    * 如果将spark.yarn.submit.waitAppCompletion设置为true,
    * 它将保持活动状态,报告应用程序的状态,直到应用程序因任何原因退出。
    * 否则,客户端进程将在提交后退出,如果应用程序以失败,
    * 终止或未定义状态完成,则抛出适当的SparkException。
   */
  def run(): Unit = {
    val appId = submitApplication()
    if (fireAndForget) {
      val report = getApplicationReport(appId)
      val state = report.getYarnApplicationState
      logInfo(s"Application report for $appId (state: $state)")
      logInfo(formatReportDetails(report))
      if (state == YarnApplicationState.FAILED || state == YarnApplicationState.KILLED) {
        throw new SparkException(s"Application $appId finished with status: $state")
      }
    } else {
      val (yarnApplicationState, finalApplicationStatus) = monitorApplication(appId)
      if (yarnApplicationState == YarnApplicationState.FAILED ||
        finalApplicationStatus == FinalApplicationStatus.FAILED) {
        throw new SparkException(s"Application $appId finished with failed status")
      }
      if (yarnApplicationState == YarnApplicationState.KILLED ||
        finalApplicationStatus == FinalApplicationStatus.KILLED) {
        throw new SparkException(s"Application $appId is killed")
      }
      if (finalApplicationStatus == FinalApplicationStatus.UNDEFINED) {
        throw new SparkException(s"The final status of application $appId is undefined")
      }
    }
  }

  private def findPySparkArchives(): Seq[String] = {
    //System.getenv()和System.getProperties()的区别
    //System.getenv() 返回系统环境变量值 设置系统环境变量：当前登录用户主目录下的".bashrc"文件中可以设置系统环境变量
    //System.getProperties() 返回Java进程变量值 通过命令行参数的"-D"选项
    sys.env.get("PYSPARK_ARCHIVES_PATH")
      .map(_.split(",").toSeq)
      .getOrElse {
        val pyLibPath = Seq(sys.env("SPARK_HOME"), "python", "lib").mkString(File.separator)
        val pyArchivesFile = new File(pyLibPath, "pyspark.zip")
        require(pyArchivesFile.exists(),
          "pyspark.zip not found; cannot run pyspark application in YARN mode.")
        val py4jFile = new File(pyLibPath, "py4j-0.8.2.1-src.zip")
        require(py4jFile.exists(),
          "py4j-0.8.2.1-src.zip not found; cannot run pyspark application in YARN mode.")
        Seq(pyArchivesFile.getAbsolutePath(), py4jFile.getAbsolutePath())
      }
  }

}

object Client extends Logging {
  def main(argStrings: Array[String]) {
    if (!sys.props.contains("SPARK_SUBMIT")) {
      logWarning("WARNING: This client is deprecated and will be removed in a " +
        "future version of Spark. Use ./bin/spark-submit with \"--master yarn\"")
    }

    // Set an env variable indicating we are running in YARN mode.
    //设置一个env变量,表示我们正在YARN模式下运行
    // Note that any env variable with the SPARK_ prefix gets propagated to all (remote) processes
    //请注意,具有SPARK_前缀的任何env变量都会传播到所有(远程)进程
    System.setProperty("SPARK_YARN_MODE", "true")
    val sparkConf = new SparkConf

    val args = new ClientArguments(argStrings, sparkConf)
    // to maintain backwards-compatibility
    //保持向后兼容性
    if (!Utils.isDynamicAllocationEnabled(sparkConf)) {
      sparkConf.setIfMissing("spark.executor.instances", args.numExecutors.toString)
    }
    new Client(args, sparkConf).run()
  }

  // Alias for the Spark assembly jar and the user jar
  //Spark程序集jar和用户jar的别名
  val SPARK_JAR: String = "__spark__.jar"
  val APP_JAR: String = "__app__.jar"

  // URI scheme that identifies local resources
  //标识本地资源的URI方案
  val LOCAL_SCHEME = "local"

  // Staging directory for any temporary jars or files
  //任何临时jar或文件的暂存目录
  val SPARK_STAGING: String = ".sparkStaging"

  // Location of any user-defined Spark jars
  //任何用户定义的Spark jar的位置
  val CONF_SPARK_JAR = "spark.yarn.jar"
  val ENV_SPARK_JAR = "SPARK_JAR"

  // Internal config to propagate the location of the user's jar to the driver/executors
  //内部配置将用户jar的位置传播给驱动程序/执行程序
  val CONF_SPARK_USER_JAR = "spark.yarn.user.jar"

  // Internal config to propagate the locations of any extra jars to add to the classpath
  // of the executors
  //内部配置,用于传播任何额外jar的位置,以添加到执行程序的类路径中
  val CONF_SPARK_YARN_SECONDARY_JARS = "spark.yarn.secondary.jars"

  // Staging directory is private! -> rwx--------
  //暂存目录是私有的！ - > rwx --------
  val STAGING_DIR_PERMISSION: FsPermission =
    FsPermission.createImmutable(Integer.parseInt("700", 8).toShort)

  // App files are world-wide readable and owner writable -> rw-r--r--
  //应用程序文件在世界范围内可读并且所有者可写 - > rw-r - r--
  val APP_FILE_PERMISSION: FsPermission =
    FsPermission.createImmutable(Integer.parseInt("644", 8).toShort)

  // Distribution-defined classpath to add to processes
  //要添加到进程的分发定义的类路径
  val ENV_DIST_CLASSPATH = "SPARK_DIST_CLASSPATH"

  // Subdirectory where the user's Spark and Hadoop config files will be placed.
  //将放置用户的Spark和Hadoop配置文件的子目录
  val LOCALIZED_CONF_DIR = "__spark_conf__"

  // Name of the file in the conf archive containing Spark configuration.
  //包含Spark配置的conf存档中文件的名称
  val SPARK_CONF_FILE = "__spark_conf__.properties"

  // Subdirectory where the user's python files (not archives) will be placed.
  //将放置用户的python文件(不是存档)的子目录
  val LOCALIZED_PYTHON_DIR = "__pyfiles__"

  /**
   * Find the user-defined Spark jar if configured, or return the jar containing this
   * class if not.
    * 如果已配置,请查找用户定义的Spark jar,否则返回包含此类的jar
   *
   * This method first looks in the SparkConf object for the CONF_SPARK_JAR key, and in the
   * user environment if that is not found (for backwards compatibility).
    * 此方法首先在SparkConf对象中查找CONF_SPARK_JAR键,如果未找到,则在用户环境中查找(为了向后兼容)。
   */
  private def sparkJar(conf: SparkConf): String = {
    if (conf.contains(CONF_SPARK_JAR)) {
      conf.get(CONF_SPARK_JAR)
    } else if (System.getenv(ENV_SPARK_JAR) != null) {
      logWarning(
        s"$ENV_SPARK_JAR detected in the system environment. This variable has been deprecated " +
          s"in favor of the $CONF_SPARK_JAR configuration variable.")
      System.getenv(ENV_SPARK_JAR)
    } else {
      SparkContext.jarOfClass(this.getClass).head
    }
  }

  /**
   * Return the path to the given application's staging directory.
    * 返回给定应用程序的临时目录的路径
   */
  private def getAppStagingDir(appId: ApplicationId): String = {
    buildPath(SPARK_STAGING, appId.toString())
  }

  /**
   * Populate the classpath entry in the given environment map with any application
   * classpath specified through the Hadoop and Yarn configurations.
    * 使用通过Hadoop和Yarn配置指定的任何应用程序类路径填充给定环境映射中的类路径条目
   */
  private[yarn] def populateHadoopClasspath(conf: Configuration, env: HashMap[String, String])
    : Unit = {
    val classPathElementsToAdd = getYarnAppClasspath(conf) ++ getMRAppClasspath(conf)
    for (c <- classPathElementsToAdd.flatten) {
      YarnSparkHadoopUtil.addPathToEnvironment(env, Environment.CLASSPATH.name, c.trim)
    }
  }

  private def getYarnAppClasspath(conf: Configuration): Option[Seq[String]] =
    Option(conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH)) match {
      case Some(s) => Some(s.toSeq)
      case None => getDefaultYarnApplicationClasspath
    }

  private def getMRAppClasspath(conf: Configuration): Option[Seq[String]] =
    Option(conf.getStrings("mapreduce.application.classpath")) match {
      case Some(s) => Some(s.toSeq)
      case None => getDefaultMRApplicationClasspath
    }

  private[yarn] def getDefaultYarnApplicationClasspath: Option[Seq[String]] = {
    val triedDefault = Try[Seq[String]] {
      val field = classOf[YarnConfiguration].getField("DEFAULT_YARN_APPLICATION_CLASSPATH")
      val value = field.get(null).asInstanceOf[Array[String]]
      value.toSeq
    } recoverWith {
      case e: NoSuchFieldException => Success(Seq.empty[String])
    }

    triedDefault match {
      case f: Failure[_] =>
        logError("Unable to obtain the default YARN Application classpath.", f.exception)
      case s: Success[Seq[String]] =>
        logDebug(s"Using the default YARN application classpath: ${s.get.mkString(",")}")
    }

    triedDefault.toOption
  }

  /**
   * In Hadoop 0.23, the MR application classpath comes with the YARN application
   * classpath. In Hadoop 2.0, it's an array of Strings, and in 2.2+ it's a String.
   * So we need to use reflection to retrieve it.
    * 在Hadoop 0.23中,MR应用程序类路径随YARN应用程序类路径一起提供,
    * 在Hadoop 2.0中,它是一个字符串数组，在2.2+中它是一个字符串,所以我们需要使用反射来检索它
   */
  private[yarn] def getDefaultMRApplicationClasspath: Option[Seq[String]] = {
    val triedDefault = Try[Seq[String]] {
      val field = classOf[MRJobConfig].getField("DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH")
      val value = if (field.getType == classOf[String]) {
        StringUtils.getStrings(field.get(null).asInstanceOf[String]).toArray
      } else {
        field.get(null).asInstanceOf[Array[String]]
      }
      value.toSeq
    } recoverWith {
      case e: NoSuchFieldException => Success(Seq.empty[String])
    }

    triedDefault match {
      case f: Failure[_] =>
        logError("Unable to obtain the default MR Application classpath.", f.exception)
      case s: Success[Seq[String]] =>
        logDebug(s"Using the default MR application classpath: ${s.get.mkString(",")}")
    }

    triedDefault.toOption
  }

  /**
   * Populate the classpath entry in the given environment map.
   * 填充给定环境映射中的类路径条目
   * User jars are generally not added to the JVM's system classpath; those are handled by the AM
   * and executor backend. When the deprecated `spark.yarn.user.classpath.first` is used, user jars
   * are included in the system classpath, though. The extra class path and other uploaded files are
   * always made available through the system class path.
    *
    * 用户jar通常不会添加到JVM的系统类路径中; 这些由AM和执行程序后端处理,
    * 当使用不推荐使用的`spark.yarn.user.classpath.first`时,用户jar包含在系统类路径中,
    * 额外的类路径和其他上载的文件始终通过系统类路径提供。
   *
   * @param args Client arguments (when starting the AM) or null (when starting executors).
   */
  private[yarn] def populateClasspath(
      args: ClientArguments,
      conf: Configuration,
      sparkConf: SparkConf,
      env: HashMap[String, String],
      isAM: Boolean,
      extraClassPath: Option[String] = None): Unit = {
    extraClassPath.foreach { cp =>
      addClasspathEntry(getClusterPath(sparkConf, cp), env)
    }
    addClasspathEntry(YarnSparkHadoopUtil.expandEnvironment(Environment.PWD), env)

    if (isAM) {
      addClasspathEntry(
        YarnSparkHadoopUtil.expandEnvironment(Environment.PWD) + Path.SEPARATOR +
          LOCALIZED_CONF_DIR, env)
    }

    if (sparkConf.getBoolean("spark.yarn.user.classpath.first", false)) {
      // in order to properly add the app jar when user classpath is first
      // we have to do the mainJar separate in order to send the right thing
      // into addFileToClasspath
      //为了在用户类路径首先正确添加app jar,我们必须将mainJar分开,
      //以便将正确的东西发送到addFileToClasspath
      val mainJar =
        if (args != null) {
          getMainJarUri(Option(args.userJar))
        } else {
          getMainJarUri(sparkConf.getOption(CONF_SPARK_USER_JAR))
        }
      mainJar.foreach(addFileToClasspath(sparkConf, conf, _, APP_JAR, env))

      val secondaryJars =
        if (args != null) {
          getSecondaryJarUris(Option(args.addJars))
        } else {
          getSecondaryJarUris(sparkConf.getOption(CONF_SPARK_YARN_SECONDARY_JARS))
        }
      secondaryJars.foreach { x =>
        addFileToClasspath(sparkConf, conf, x, null, env)
      }
    }
    addFileToClasspath(sparkConf, conf, new URI(sparkJar(sparkConf)), SPARK_JAR, env)
    populateHadoopClasspath(conf, env)
    //System.getenv()和System.getProperties()的区别
    //System.getenv() 返回系统环境变量值 设置系统环境变量：当前登录用户主目录下的".bashrc"文件中可以设置系统环境变量
    //System.getProperties() 返回Java进程变量值 通过命令行参数的"-D"选项
    sys.env.get(ENV_DIST_CLASSPATH).foreach { cp =>
      addClasspathEntry(getClusterPath(sparkConf, cp), env)
    }
  }

  /**
   * Returns a list of URIs representing the user classpath.
   * 返回表示用户类路径的URI列表
   * @param conf Spark configuration.
   */
  def getUserClasspath(conf: SparkConf): Array[URI] = {
    val mainUri = getMainJarUri(conf.getOption(CONF_SPARK_USER_JAR))
    val secondaryUris = getSecondaryJarUris(conf.getOption(CONF_SPARK_YARN_SECONDARY_JARS))
    (mainUri ++ secondaryUris).toArray
  }

  private def getMainJarUri(mainJar: Option[String]): Option[URI] = {
    mainJar.flatMap { path =>
      val uri = Utils.resolveURI(path)
      if (uri.getScheme == LOCAL_SCHEME) Some(uri) else None
    }.orElse(Some(new URI(APP_JAR)))
  }

  private def getSecondaryJarUris(secondaryJars: Option[String]): Seq[URI] = {
    secondaryJars.map(_.split(",")).toSeq.flatten.map(new URI(_))
  }

  /**
   * Adds the given path to the classpath, handling "local:" URIs correctly.
   * 将给定路径添加到类路径,正确处理“local：”URI
   * If an alternate name for the file is given, and it's not a "local:" file, the alternate
   * name will be added to the classpath (relative to the job's work directory).
   * 如果给出了文件的备用名称,并且它不是“local：”文件,则备用名称将添加到类路径中(相对于作业的工作目录)
    *
   * If not a "local:" file and no alternate name, the linkName will be added to the classpath.
    * 如果不是“local：”文件而没有备用名称,则linkName将添加到类路径中
   *
   * @param conf        Spark configuration.
   * @param hadoopConf  Hadoop configuration.
   * @param uri         URI to add to classpath (optional).
   * @param fileName    Alternate name for the file (optional).
   * @param env         Map holding the environment variables.
   */
  private def addFileToClasspath(
      conf: SparkConf,
      hadoopConf: Configuration,
      uri: URI,
      fileName: String,
      env: HashMap[String, String]): Unit = {
    if (uri != null && uri.getScheme == LOCAL_SCHEME) {
      addClasspathEntry(getClusterPath(conf, uri.getPath), env)
    } else if (fileName != null) {
      addClasspathEntry(buildPath(
        YarnSparkHadoopUtil.expandEnvironment(Environment.PWD), fileName), env)
    } else if (uri != null) {
      val localPath = getQualifiedLocalPath(uri, hadoopConf)
      val linkName = Option(uri.getFragment()).getOrElse(localPath.getName())
      addClasspathEntry(buildPath(
        YarnSparkHadoopUtil.expandEnvironment(Environment.PWD), linkName), env)
    }
  }

  /**
   * Add the given path to the classpath entry of the given environment map.
    * 将给定路径添加到给定环境映射的类路径条目
   * If the classpath is already set, this appends the new path to the existing classpath.
    * 如果已设置类路径,则会将新路径附加到现有类路径
   */
  private def addClasspathEntry(path: String, env: HashMap[String, String]): Unit =
    YarnSparkHadoopUtil.addPathToEnvironment(env, Environment.CLASSPATH.name, path)

  /**
   * Returns the path to be sent to the NM for a path that is valid on the gateway.
   * 返回要在网关上有效的路径发送到NM的路径
   * This method uses two configuration values:
   * 此方法使用两个配置值：
   * - spark.yarn.config.gatewayPath: a string that identifies a portion of the input path that may
   *   only be valid in the gateway node.
    *   spark.yarn.config.gatewayPath:一个字符串,用于标识可能仅在网关节点中有效的输入路径的一部分。
    *
   * - spark.yarn.config.replacementPath: a string with which to replace the gateway path. This may
   *   contain, for example, env variable references, which will be expanded by the NMs when
   *   starting containers.
    * - spark.yarn.config.replacementPath：用于替换网关路径的字符串,这可能包含,
    *           例如:env变量引用,当NM时将由NM扩展起始容器。
   * If either config is not available, the input path is returned.
    * 如果任一配置不可用,则返回输入路径
   */
  def getClusterPath(conf: SparkConf, path: String): String = {
    val localPath = conf.get("spark.yarn.config.gatewayPath", null)
    val clusterPath = conf.get("spark.yarn.config.replacementPath", null)
    if (localPath != null && clusterPath != null) {
      path.replace(localPath, clusterPath)
    } else {
      path
    }
  }

  /**
   * Obtains token for the Hive metastore and adds them to the credentials.
    * 获取Hive Metastore的令牌并将其添加到凭据
   */
  private def obtainTokenForHiveMetastore(
      sparkConf: SparkConf,
      conf: Configuration,
      credentials: Credentials) {
    if (UserGroupInformation.isSecurityEnabled) {
      YarnSparkHadoopUtil.get.obtainTokenForHiveMetastore(conf).foreach {
        credentials.addToken(new Text("hive.server2.delegation.token"), _)
      }
    }
  }

  /**
   * Obtain security token for HBase.
    * 获取HBase的安全令牌
   */
  def obtainTokenForHBase(conf: Configuration, credentials: Credentials): Unit = {
    if (UserGroupInformation.isSecurityEnabled) {
      val mirror = universe.runtimeMirror(getClass.getClassLoader)

      try {
        val confCreate = mirror.classLoader.
          loadClass("org.apache.hadoop.hbase.HBaseConfiguration").
          getMethod("create", classOf[Configuration])
        val obtainToken = mirror.classLoader.
          loadClass("org.apache.hadoop.hbase.security.token.TokenUtil").
          getMethod("obtainToken", classOf[Configuration])

        logDebug("Attempting to fetch HBase security token.")

        val hbaseConf = confCreate.invoke(null, conf).asInstanceOf[Configuration]
        if ("kerberos" == hbaseConf.get("hbase.security.authentication")) {
          val token = obtainToken.invoke(null, hbaseConf).asInstanceOf[Token[TokenIdentifier]]
          credentials.addToken(token.getService, token)
          logInfo("Added HBase security token to credentials.")
        }
      } catch {
        case e: java.lang.NoSuchMethodException =>
          logInfo("HBase Method not found: " + e)
        case e: java.lang.ClassNotFoundException =>
          logDebug("HBase Class not found: " + e)
        case e: java.lang.NoClassDefFoundError =>
          logDebug("HBase Class not found: " + e)
        case e: Exception =>
          logError("Exception when obtaining HBase security token: " + e)
      }
    }
  }

  /**
   * Return whether the two file systems are the same.
    * 返回两个文件系统是否相同
   */
  private def compareFs(srcFs: FileSystem, destFs: FileSystem): Boolean = {
    val srcUri = srcFs.getUri()
    val dstUri = destFs.getUri()
    if (srcUri.getScheme() == null || srcUri.getScheme() != dstUri.getScheme()) {
      return false
    }

    var srcHost = srcUri.getHost()
    var dstHost = dstUri.getHost()

    // In HA or when using viewfs, the host part of the URI may not actually be a host, but the
    // name of the HDFS namespace. Those names won't resolve, so avoid even trying if they
    // match.
    //在HA或使用viewfs时,URI的主机部分实际上可能不是主机,而是HDFS命名空间的名称,
    //这些名称将无法解决,因此如果匹配,请避免尝试
    if (srcHost != null && dstHost != null && srcHost != dstHost) {
      try {
        srcHost = InetAddress.getByName(srcHost).getCanonicalHostName()
        dstHost = InetAddress.getByName(dstHost).getCanonicalHostName()
      } catch {
        case e: UnknownHostException =>
          return false
      }
    }

    Objects.equal(srcHost, dstHost) && srcUri.getPort() == dstUri.getPort()
  }

  /**
   * Given a local URI, resolve it and return a qualified local path that corresponds to the URI.
    * 给定本地URI,解析它并返回与URI对应的限定本地路径。
   * This is used for preparing local resources to be included in the container launch context.
    * 这用于准备要包含在容器启动上下文中的本地资源
   */
  private def getQualifiedLocalPath(localURI: URI, hadoopConf: Configuration): Path = {
    val qualifiedURI =
      if (localURI.getScheme == null) {
        // If not specified, assume this is in the local filesystem to keep the behavior
        // consistent with that of Hadoop
        //如果未指定,则假设这是在本地文件系统中,以保持行为与Hadoop的行为一致
        new URI(FileSystem.getLocal(hadoopConf).makeQualified(new Path(localURI)).toString)
      } else {
        localURI
      }
    new Path(qualifiedURI)
  }

  /**
   * Whether to consider jars provided by the user to have precedence over the Spark jars when
   * loading user classes.
    * 是否考虑用户提供的jar在加载用户类时优先于Spark jar
   */
  def isUserClassPathFirst(conf: SparkConf, isDriver: Boolean): Boolean = {
    if (isDriver) {
      conf.getBoolean("spark.driver.userClassPathFirst", false)
    } else {
      conf.getBoolean("spark.executor.userClassPathFirst", false)
    }
  }

  /**
   * Joins all the path components using Path.SEPARATOR.
    * 使用Path.SEPARATOR连接所有路径组件
   */
  def buildPath(components: String*): String = {
    components.mkString(Path.SEPARATOR)
  }

}
