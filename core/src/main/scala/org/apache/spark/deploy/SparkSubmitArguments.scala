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

package org.apache.spark.deploy

import java.io.{ByteArrayOutputStream, PrintStream}
import java.lang.reflect.InvocationTargetException
import java.net.URI
import java.util.{List => JList}
import java.util.jar.JarFile

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.io.Source

import org.apache.spark.deploy.SparkSubmitAction._
import org.apache.spark.launcher.SparkSubmitArgumentsParser
import org.apache.spark.util.Utils

/**
 * Parses and encapsulates arguments from the spark-submit script.
 * 解析Spark-submit 脚本参数
 * The env argument is used for testing.
 */
private[deploy] class SparkSubmitArguments(args: Seq[String], env: Map[String, String] = sys.env)
  extends SparkSubmitArgumentsParser {
  var master: String = null
  var deployMode: String = null
  var executorMemory: String = null
  var executorCores: String = null
  var totalExecutorCores: String = null
  var propertiesFile: String = null
  var driverMemory: String = null
  var driverExtraClassPath: String = null
  var driverExtraLibraryPath: String = null
  var driverExtraJavaOptions: String = null
  var queue: String = null
  var numExecutors: String = null
  var files: String = null
  var archives: String = null
  var mainClass: String = null
  var primaryResource: String = null
  var name: String = null
  var childArgs: ArrayBuffer[String] = new ArrayBuffer[String]()
  var jars: String = null
  var packages: String = null
  var repositories: String = null
  var ivyRepoPath: String = null
  var packagesExclusions: String = null
  var verbose: Boolean = false
  var isPython: Boolean = false
  var pyFiles: String = null
  var isR: Boolean = false
  var action: SparkSubmitAction = null
  val sparkProperties: HashMap[String, String] = new HashMap[String, String]()
  var proxyUser: String = null
  var principal: String = null
  var keytab: String = null

  // Standalone cluster mode only
  var supervise: Boolean = false
  var driverCores: String = null
  var submissionToKill: String = null
  var submissionToRequestStatusFor: String = null
  var useRest: Boolean = true // used internally

  /** 
   *  Default properties present in the currently defined defaults file.
   *  定义的默认属性文件
   *   */
  lazy val defaultSparkProperties: HashMap[String, String] = {
    val defaultProperties = new HashMap[String, String]()
    // scalastyle:off println
    if (verbose) SparkSubmit.printStream.println(s"Using properties file: $propertiesFile")
    Option(propertiesFile).foreach { filename =>
      Utils.getPropertiesFromFile(filename).foreach { case (k, v) =>
        defaultProperties(k) = v
        if (verbose) SparkSubmit.printStream.println(s"Adding default property: $k=$v")
      }
    }
    // scalastyle:on println
    defaultProperties
  }

  // Set parameters from command line arguments
  //设置命令行参数
  try {
    parse(args.toList)
  } catch {
    case e: IllegalArgumentException =>
      SparkSubmit.printErrorAndExit(e.getMessage())
  }
  // Populate `sparkProperties` map from properties file
  //从属性文件填充`sparkProperties`映射
  mergeDefaultSparkProperties()
  // Remove keys that don't start with "spark." from `sparkProperties`.
  //删除不以“spark”开头的键。 来自`sparkProperties`
  ignoreNonSparkProperties()
  // Use `sparkProperties` map along with env vars to fill in any missing parameters
  //使用`sparkProperties'映射和env vars来填充任何缺少的参数
  loadEnvironmentArguments()

  validateArguments()

  /**
   * Merge values from the default properties file with those specified through --conf.
   * When this is called, `sparkProperties` is already filled with configs from the latter.
   * 合并值与那些通过指定文件的默认属性--配置文件,调用"sparkProperties"已经装满了从后者的配置
   */
  private def mergeDefaultSparkProperties(): Unit = {
    // Use common defaults file, if not specified by user
    //使用普通的默认文件,如果用户没有指定
    propertiesFile = Option(propertiesFile).getOrElse(Utils.getDefaultPropertiesFile(env))
    // Honor --conf before the defaults file
    //荣誉--conf之前的默认文件
    defaultSparkProperties.foreach { case (k, v) =>
      if (!sparkProperties.contains(k)) {
        sparkProperties(k) = v
      }
    }
  }

  /**
   * Remove keys that don't start with "spark." from `sparkProperties`.
   * 移除不是spark.开头
   */
  private def ignoreNonSparkProperties(): Unit = {
    sparkProperties.foreach { case (k, v) =>
      if (!k.startsWith("spark.")) {
        sparkProperties -= k
        SparkSubmit.printWarning(s"Ignoring non-spark config property: $k=$v")
      }
    }
  }

  /**
   * Load arguments from environment variables, Spark properties etc.
   * 从环境变量中的加载参数,Spark属性文件等
   */
  private def loadEnvironmentArguments(): Unit = {
    master = Option(master)
      .orElse(sparkProperties.get("spark.master"))
      .orElse(env.get("MASTER"))
      .orNull
    driverExtraClassPath = Option(driverExtraClassPath)
      .orElse(sparkProperties.get("spark.driver.extraClassPath"))
      .orNull
    driverExtraJavaOptions = Option(driverExtraJavaOptions)
      .orElse(sparkProperties.get("spark.driver.extraJavaOptions"))
      .orNull
    driverExtraLibraryPath = Option(driverExtraLibraryPath)
      .orElse(sparkProperties.get("spark.driver.extraLibraryPath"))
      .orNull
      //Spark应用程序Application所占的内存大小
    driverMemory = Option(driverMemory)
      .orElse(sparkProperties.get("spark.driver.memory"))
      .orElse(env.get("SPARK_DRIVER_MEMORY"))
      .orNull
      //Spark应用程序Application所占用的CPU核的数目；
    driverCores = Option(driverCores)
      .orElse(sparkProperties.get("spark.driver.cores"))
      .orNull
      //分配给每个executor进程总内存
    executorMemory = Option(executorMemory)
      .orElse(sparkProperties.get("spark.executor.memory"))
      .orElse(env.get("SPARK_EXECUTOR_MEMORY"))
      .orNull
      //每个Worker所占用的CPU核的数目
    executorCores = Option(executorCores)
      .orElse(sparkProperties.get("spark.executor.cores"))
      .orElse(env.get("SPARK_EXECUTOR_CORES"))
      .orNull
   //当运行在一个独立部署集群上或者是一个粗粒度共享模式的Mesos集群上的时候,最多可以请求多少个CPU核心。默认是所有的都能用
    totalExecutorCores = Option(totalExecutorCores)
      .orElse(sparkProperties.get("spark.cores.max"))
      .orNull
      
    name = Option(name).orElse(sparkProperties.get("spark.app.name")).orNull//如果它不为空返回该选项的值,如果它是空则返回null
    jars = Option(jars).orElse(sparkProperties.get("spark.jars")).orNull//如果它不为空返回该选项的值,如果它是空则返回null
    ivyRepoPath = sparkProperties.get("spark.jars.ivy").orNull//如果它不为空返回该选项的值,如果它是空则返回null
    packages = Option(packages).orElse(sparkProperties.get("spark.jars.packages")).orNull//如果它不为空返回该选项的值,如果它是空则返回null
    packagesExclusions = Option(packagesExclusions)
      .orElse(sparkProperties.get("spark.jars.excludes")).orNull
    deployMode = Option(deployMode).orElse(env.get("DEPLOY_MODE")).orNull
    numExecutors = Option(numExecutors)
      .getOrElse(sparkProperties.get("spark.executor.instances").orNull)
    keytab = Option(keytab).orElse(sparkProperties.get("spark.yarn.keytab")).orNull
    principal = Option(principal).orElse(sparkProperties.get("spark.yarn.principal")).orNull

    // Try to set main class from JAR if no --class argument is given
    //尝试从JAR设置主类，如果没有给出--class参数
    if (mainClass == null && !isPython && !isR && primaryResource != null) {
      val uri = new URI(primaryResource)
      val uriScheme = uri.getScheme()

      uriScheme match {
        case "file" =>
          try {
            val jar = new JarFile(uri.getPath)
            // Note that this might still return null if no main-class is set; we catch that later
            //注意，如果没有设置main类,这可能仍然返回null;我们以后赶上
            mainClass = jar.getManifest.getMainAttributes.getValue("Main-Class")
          } catch {
            case e: Exception =>
              SparkSubmit.printErrorAndExit(s"Cannot load main class from JAR $primaryResource")
          }
        case _ =>
          SparkSubmit.printErrorAndExit(
            s"Cannot load main class from JAR $primaryResource with URI $uriScheme. " +
            "Please specify a class through --class.")
      }
    }

    // Global defaults. These should be keep to minimum to avoid confusing behavior.
    //全局默认值,这些应该保持最小,以避免混乱的行为。
    master = Option(master).getOrElse("local[*]")

    // In YARN mode, app name can be set via SPARK_YARN_APP_NAME (see SPARK-5222)
    //在YARN模式下,可以通过SPARK_YARN_APP_NAME设置应用名称（请参阅SPARK-5222）
    if (master.startsWith("yarn")) {
      name = Option(name).orElse(env.get("SPARK_YARN_APP_NAME")).orNull
    }

    // Set name from main class if not given
    //如果没有给出,从主类设置名称
    name = Option(name).orElse(Option(mainClass)).orNull
    if (name == null && primaryResource != null) {
      name = Utils.stripDirectory(primaryResource)
    }

    // Action should be SUBMIT unless otherwise specified
    //除非另有规定,否则动作应为提交
    action = Option(action).getOrElse(SUBMIT)
  }

  /** 
   *  Ensure that required fields exists. Call this only once all defaults are loaded. 
   *  确保所需字段的存在,调用只有一次所有的默认加载
   *  */
  private def validateArguments(): Unit = {
    action match {
      case SUBMIT => validateSubmitArguments()
      case KILL => validateKillArguments()
      case REQUEST_STATUS => validateStatusRequestArguments()
    }
  }

  private def validateSubmitArguments(): Unit = {
    if (args.length == 0) {
      printUsageAndExit(-1)
    }
    if (primaryResource == null) {
      SparkSubmit.printErrorAndExit("Must specify a primary resource (JAR or Python or R file)")
    }
    if (mainClass == null && SparkSubmit.isUserJar(primaryResource)) {
      SparkSubmit.printErrorAndExit("No main class set in JAR; please specify one with --class")
    }
    if (pyFiles != null && !isPython) {
      SparkSubmit.printErrorAndExit("--py-files given but primary resource is not a Python script")
    }

    if (master.startsWith("yarn")) {
      val hasHadoopEnv = env.contains("HADOOP_CONF_DIR") || env.contains("YARN_CONF_DIR")
      if (!hasHadoopEnv && !Utils.isTesting) {
        throw new Exception(s"When running with master '$master' " +
          "either HADOOP_CONF_DIR or YARN_CONF_DIR must be set in the environment.")
      }
    }
  }

  private def validateKillArguments(): Unit = {
    if (!master.startsWith("spark://") && !master.startsWith("mesos://")) {
      SparkSubmit.printErrorAndExit(
        "Killing submissions is only supported in standalone or Mesos mode!")
    }
    if (submissionToKill == null) {
      SparkSubmit.printErrorAndExit("Please specify a submission to kill.")
    }
  }

  private def validateStatusRequestArguments(): Unit = {
    if (!master.startsWith("spark://") && !master.startsWith("mesos://")) {
      SparkSubmit.printErrorAndExit(
        "Requesting submission statuses is only supported in standalone or Mesos mode!")
    }
    if (submissionToRequestStatusFor == null) {
      SparkSubmit.printErrorAndExit("Please specify a submission to request status for.")
    }
  }

  def isStandaloneCluster: Boolean = {
    master.startsWith("spark://") && deployMode == "cluster"
  }

  override def toString: String = {
    s"""Parsed arguments:
    |  master                  $master
    |  deployMode              $deployMode
    |  executorMemory          $executorMemory
    |  executorCores           $executorCores
    |  totalExecutorCores      $totalExecutorCores
    |  propertiesFile          $propertiesFile
    |  driverMemory            $driverMemory
    |  driverCores             $driverCores
    |  driverExtraClassPath    $driverExtraClassPath
    |  driverExtraLibraryPath  $driverExtraLibraryPath
    |  driverExtraJavaOptions  $driverExtraJavaOptions
    |  supervise               $supervise
    |  queue                   $queue
    |  numExecutors            $numExecutors
    |  files                   $files
    |  pyFiles                 $pyFiles
    |  archives                $archives
    |  mainClass               $mainClass
    |  primaryResource         $primaryResource
    |  name                    $name
    |  childArgs               [${childArgs.mkString(" ")}]
    |  jars                    $jars
    |  packages                $packages
    |  packagesExclusions      $packagesExclusions
    |  repositories            $repositories
    |  verbose                 $verbose
    |
    |Spark properties used, including those specified through
    | --conf and those from the properties file $propertiesFile:
    |${sparkProperties.mkString("  ", "\n  ", "\n")}
    """.stripMargin
  }

  /** Fill in values by parsing user options.
    * 通过解析用户选项来填充值 */
  override protected def handle(opt: String, value: String): Boolean = {
    opt match {
      case NAME =>
        name = value

      case MASTER =>
        master = value

      case CLASS =>
        mainClass = value

      case DEPLOY_MODE =>
        if (value != "client" && value != "cluster") {
          SparkSubmit.printErrorAndExit("--deploy-mode must be either \"client\" or \"cluster\"")
        }
        deployMode = value

      case NUM_EXECUTORS =>
        numExecutors = value

      case TOTAL_EXECUTOR_CORES =>
        totalExecutorCores = value

      case EXECUTOR_CORES =>
        executorCores = value

      case EXECUTOR_MEMORY =>
        executorMemory = value

      case DRIVER_MEMORY =>
        driverMemory = value

      case DRIVER_CORES =>
        driverCores = value

      case DRIVER_CLASS_PATH =>
        driverExtraClassPath = value

      case DRIVER_JAVA_OPTIONS =>
        driverExtraJavaOptions = value

      case DRIVER_LIBRARY_PATH =>
        driverExtraLibraryPath = value

      case PROPERTIES_FILE =>
        propertiesFile = value

      case KILL_SUBMISSION =>
        submissionToKill = value
        if (action != null) {
          SparkSubmit.printErrorAndExit(s"Action cannot be both $action and $KILL.")
        }
        action = KILL

      case STATUS =>
        submissionToRequestStatusFor = value
        if (action != null) {
          SparkSubmit.printErrorAndExit(s"Action cannot be both $action and $REQUEST_STATUS.")
        }
        action = REQUEST_STATUS

      case SUPERVISE =>
        supervise = true

      case QUEUE =>
        queue = value

      case FILES =>
        files = Utils.resolveURIs(value)

      case PY_FILES =>
        pyFiles = Utils.resolveURIs(value)

      case ARCHIVES =>
        archives = Utils.resolveURIs(value)

      case JARS =>
        jars = Utils.resolveURIs(value)

      case PACKAGES =>
        packages = value

      case PACKAGES_EXCLUDE =>
        packagesExclusions = value

      case REPOSITORIES =>
        repositories = value

      case CONF =>
        value.split("=", 2).toSeq match {
          case Seq(k, v) => sparkProperties(k) = v
          case _ => SparkSubmit.printErrorAndExit(s"Spark config without '=': $value")
        }

      case PROXY_USER =>
        proxyUser = value

      case PRINCIPAL =>
        principal = value

      case KEYTAB =>
        keytab = value

      case HELP =>
        printUsageAndExit(0)

      case VERBOSE =>
        verbose = true

      case VERSION =>
        SparkSubmit.printVersionAndExit()

      case USAGE_ERROR =>
        printUsageAndExit(1)

      case _ =>
        throw new IllegalArgumentException(s"Unexpected argument '$opt'.")
    }
    true
  }

  /**
   * Handle unrecognized command line options.
   * 处理无法识别的命令行选项
   * The first unrecognized option is treated as the "primary resource". Everything else is
   * treated as application arguments.
    * 第一个无法识别的选项被视为“主要资源”,一切都被视为应用程序参数
   */
  override protected def handleUnknown(opt: String): Boolean = {
    if (opt.startsWith("-")) {
      SparkSubmit.printErrorAndExit(s"Unrecognized option '$opt'.")
    }

    primaryResource =
      if (!SparkSubmit.isShell(opt) && !SparkSubmit.isInternal(opt)) {
        Utils.resolveURI(opt).toString
      } else {
        opt
      }
    isPython = SparkSubmit.isPython(opt)
    isR = SparkSubmit.isR(opt)
    false
  }

  override protected def handleExtraArgs(extra: JList[String]): Unit = {
    childArgs ++= extra
  }

  private def printUsageAndExit(exitCode: Int, unknownParam: Any = null): Unit = {
    // scalastyle:off println
    val outStream = SparkSubmit.printStream
    if (unknownParam != null) {
      outStream.println("Unknown/unsupported param " + unknownParam)
    }
    val command = sys.env.get("_SPARK_CMD_USAGE").getOrElse(
      """Usage: spark-submit [options] <app jar | python file> [app arguments]
        |Usage: spark-submit --kill [submission ID] --master [spark://...]
        |Usage: spark-submit --status [submission ID] --master [spark://...]""".stripMargin)
    outStream.println(command)

    val mem_mb = Utils.DEFAULT_DRIVER_MEM_MB
    outStream.println(
      s"""
        |Options:
        |  --master MASTER_URL         spark://host:port, mesos://host:port, yarn, or local.
        |  --deploy-mode DEPLOY_MODE   Whether to launch the driver program locally ("client") or
        |                              on one of the worker machines inside the cluster ("cluster")
        |                              (Default: client).
        |  --class CLASS_NAME          Your application's main class (for Java / Scala apps).
        |  --name NAME                 A name of your application.
        |  --jars JARS                 Comma-separated list of local jars to include on the driver
        |                              and executor classpaths.
        |  --packages                  Comma-separated list of maven coordinates of jars to include
        |                              on the driver and executor classpaths. Will search the local
        |                              maven repo, then maven central and any additional remote
        |                              repositories given by --repositories. The format for the
        |                              coordinates should be groupId:artifactId:version.
        |  --exclude-packages          Comma-separated list of groupId:artifactId, to exclude while
        |                              resolving the dependencies provided in --packages to avoid
        |                              dependency conflicts.
        |  --repositories              Comma-separated list of additional remote repositories to
        |                              search for the maven coordinates given with --packages.
        |  --py-files PY_FILES         Comma-separated list of .zip, .egg, or .py files to place
        |                              on the PYTHONPATH for Python apps.
        |  --files FILES               Comma-separated list of files to be placed in the working
        |                              directory of each executor.
        |
        |  --conf PROP=VALUE           Arbitrary Spark configuration property.
        |  --properties-file FILE      Path to a file from which to load extra properties. If not
        |                              specified, this will look for conf/spark-defaults.conf.
        |
        |  --driver-memory MEM         Memory for driver (e.g. 1000M, 2G) (Default: ${mem_mb}M).
        |  --driver-java-options       Extra Java options to pass to the driver.
        |  --driver-library-path       Extra library path entries to pass to the driver.
        |  --driver-class-path         Extra class path entries to pass to the driver. Note that
        |                              jars added with --jars are automatically included in the
        |                              classpath.
        |
        |  --executor-memory MEM       Memory per executor (e.g. 1000M, 2G) (Default: 1G).
        |
        |  --proxy-user NAME           User to impersonate when submitting the application.
        |
        |  --help, -h                  Show this help message and exit
        |  --verbose, -v               Print additional debug output
        |  --version,                  Print the version of current Spark
        |
        | Spark standalone with cluster deploy mode only:
        |  --driver-cores NUM          Cores for driver (Default: 1).
        |
        | Spark standalone or Mesos with cluster deploy mode only:
        |  --supervise                 If given, restarts the driver on failure.
        |  --kill SUBMISSION_ID        If given, kills the driver specified.
        |  --status SUBMISSION_ID      If given, requests the status of the driver specified.
        |
        | Spark standalone and Mesos only:
        |  --total-executor-cores NUM  Total cores for all executors.
        |
        | Spark standalone and YARN only:
        |  --executor-cores NUM        Number of cores per executor. (Default: 1 in YARN mode,
        |                              or all available cores on the worker in standalone mode)
        |
        | YARN-only:
        |  --driver-cores NUM          Number of cores used by the driver, only in cluster mode
        |                              (Default: 1).
        |  --queue QUEUE_NAME          The YARN queue to submit to (Default: "default").
        |  --num-executors NUM         Number of executors to launch (Default: 2).
        |  --archives ARCHIVES         Comma separated list of archives to be extracted into the
        |                              working directory of each executor.
        |  --principal PRINCIPAL       Principal to be used to login to KDC, while running on
        |                              secure HDFS.
        |  --keytab KEYTAB             The full path to the file that contains the keytab for the
        |                              principal specified above. This keytab will be copied to
        |                              the node running the Application Master via the Secure
        |                              Distributed Cache, for renewing the login tickets and the
        |                              delegation tokens periodically.
      """.stripMargin
    )

    if (SparkSubmit.isSqlShell(mainClass)) {
      outStream.println("CLI options:")
      outStream.println(getSqlShellOptions())
    }
    // scalastyle:on println

    SparkSubmit.exitFn(exitCode)
  }

  /**
   * Run the Spark SQL CLI main class with the "--help" option and catch its output. Then filter
   * the results to remove unwanted lines.
    * 使用“--help”选项运行Spark SQL CLI主类并捕获其输出,然后过滤结果以删除不需要的行
   *
   * Since the CLI will call `System.exit()`, we install a security manager to prevent that call
   * from working, and restore the original one afterwards.
    * 由于CLI将调用“System.exit（）”,所以我们安装一个安全管理器来阻止该调用工作,然后恢复原来的调用。
   */
  private def getSqlShellOptions(): String = {
    val currentOut = System.out
    val currentErr = System.err
    val currentSm = System.getSecurityManager()
    try {
      val out = new ByteArrayOutputStream()
      val stream = new PrintStream(out)
      System.setOut(stream)
      System.setErr(stream)

      val sm = new SecurityManager() {
        override def checkExit(status: Int): Unit = {
          throw new SecurityException()
        }

        override def checkPermission(perm: java.security.Permission): Unit = {}
      }
      System.setSecurityManager(sm)

      try {
        Utils.classForName(mainClass).getMethod("main", classOf[Array[String]])
          .invoke(null, Array(HELP))
      } catch {
        case e: InvocationTargetException =>
          // Ignore SecurityException, since we throw it above.
          if (!e.getCause().isInstanceOf[SecurityException]) {
            throw e
          }
      }

      stream.flush()

      // Get the output and discard any unnecessary lines from it.
      //获取输出并丢弃任何不必要的行
      Source.fromString(new String(out.toByteArray())).getLines
        .filter { line =>
          !line.startsWith("log4j") && !line.startsWith("usage")
        }
        .mkString("\n")
    } finally {
      System.setSecurityManager(currentSm)
      System.setOut(currentOut)
      System.setErr(currentErr)
    }
  }
}
