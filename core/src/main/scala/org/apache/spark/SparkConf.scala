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

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable.LinkedHashSet

import org.apache.avro.{SchemaNormalization, Schema}

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.util.Utils

/**
 * Configuration for a Spark application. Used to set various Spark parameters as key-value pairs.
  * Spark应用程序的配置,用于将各种Spark参数设置为键值对
 *
 * Most of the time, you would create a SparkConf object with `new SparkConf()`, which will load
 * values from any `spark.*` Java system properties set in your application as well. In this case,
 * parameters you set directly on the `SparkConf` object take priority over system properties.
  *
  * 大多数情况下,您将使用`new SparkConf（）`创建一个SparkConf对象,它将从应用程序中设置的任何`spark. *`Java系统属性中加载值。
  * 在这种情况下,直接在`SparkConf`对象上设置的参数优先于系统属性。
 *
 * For unit tests, you can also call `new SparkConf(false)` to skip loading external settings and
 * get the same configuration no matter what the system properties are.
  *
  * 对于单元测试,你也可以调用`new SparkConf（false）'来跳过加载外部设置,并获得相同的配置,无论系统属性如何,
 *
 * All setter methods in this class support chaining. For example, you can write
 * `new SparkConf().setMaster("local").setAppName("My app").
  *
  * 此类中的所有setter方法都支持链接,例如,您可以编写new SparkConf（）。setMaster（“local”）。setAppName（“My app”）。
 *
 * Note that once a SparkConf object is passed to Spark, it is cloned and can no longer be modified
 * by the user. Spark does not support modifying the configuration at runtime.
  *
  * 请注意,一旦将SparkConf对象传递给Spark,它将被克隆,并且不能再被用户修改,Spark不支持在运行时修改配置
 *
 * @param loadDefaults whether to also load values from Java system properties
 */
class SparkConf(loadDefaults: Boolean) extends Cloneable with Logging {

  import SparkConf._

  /**
    * Create a SparkConf that loads defaults from system properties and the classpath
    *创建一个SparkConf,它从系统属性和类路径加载默认值
    * */
  def this() = this(true)
  
/**
 * 主要是通过ConcurrentHashMap来维护各种Spark的配置属性
 */
  private val settings = new ConcurrentHashMap[String, String]()

  if (loadDefaults) {
    // Load any spark.* system properties
    //加载任何以spark.* 开头的系统属性
    for ((key, value) <- Utils.getSystemProperties if key.startsWith("spark.")) {
      set(key, value)//把Key和value值增加到ConcurrentHashMap中
    }
  }

  /** Set a configuration variable. */
  /**设置配制变量**/
  def set(key: String, value: String): SparkConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key)
    }
    logDeprecationWarning(key)
    settings.put(key, value)
    this
  }

  /**
   * The master URL to connect to, such as "local" to run locally with one thread, "local[4]" to
   * run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
    *
   * 要连接的主节点的URL,例如“本地”以一个线程本地运行,“local [4]”以4个核心运行,或者“spark：// master：7077”在Spark独立群集上运行
   */
  def setMaster(master: String): SparkConf = {
    set("spark.master", master)
  }

  /** Set a name for your application. Shown in the Spark web UI. 应用程序名称,在Spark Web UI中显示*/
  def setAppName(name: String): SparkConf = {
    set("spark.app.name", name)
  }

  /**
    * Set JAR files to distribute to the cluster.
    * 设置JAR文件以分发到群集
    * */
  def setJars(jars: Seq[String]): SparkConf = {
    for (jar <- jars if (jar == null)) logWarning("null jar passed to SparkContext constructor")
    set("spark.jars", jars.filter(_ != null).mkString(","))
  }

  /**
    * Set JAR files to distribute to the cluster. (Java-friendly version.)
    * 设置JAR文件以分发到群集
    * */
  def setJars(jars: Array[String]): SparkConf = {
    setJars(jars.toSeq)
  }

  /**
   * Set an environment variable to be used when launching executors for this application.
    * 设置在启动此应用程序的执行程序时使用的环境变量。
   * These variables are stored as properties of the form spark.executorEnv.VAR_NAME
   * (for example spark.executorEnv.PATH) but this method makes them easier to set.
    * 这些变量存储为spark.executorEnv.VAR_NAME形式的属性(例如spark.executorEnv.PATH),但这种方法使它们更容易设置。
   */
  def setExecutorEnv(variable: String, value: String): SparkConf = {
    set("spark.executorEnv." + variable, value)
  }

  /**
   * Set multiple environment variables to be used when launching executors.
    * 设置启动执行程序时要使用的多个环境变量
   * These variables are stored as properties of the form spark.executorEnv.VAR_NAME
   * (for example spark.executorEnv.PATH) but this method makes them easier to set.
    * 这些变量存储为spark.executorEnv.VAR_NAME形式的属性(例如spark.executorEnv.PATH),但这种方法使它们更容易设置。
   */
  def setExecutorEnv(variables: Seq[(String, String)]): SparkConf = {
    for ((k, v) <- variables) {
      setExecutorEnv(k, v)
    }
    this
  }

  /**
   * Set multiple environment variables to be used when launching executors.
    * 设置启动执行程序时要使用的多个环境变量
   * (Java-friendly version.)
   */
  def setExecutorEnv(variables: Array[(String, String)]): SparkConf = {
    setExecutorEnv(variables.toSeq)
  }

  /**
   * Set the location where Spark is installed on worker nodes.
    * 设置Spark在工作节点上的安装位置
   */
  def setSparkHome(home: String): SparkConf = {
    set("spark.home", home)
  }

  /**
    * Set multiple parameters together
    * 设置多个参数
    * */
  def setAll(settings: Traversable[(String, String)]): SparkConf = {
    settings.foreach { case (k, v) => set(k, v) }
    this
  }

  /**
    * Set a parameter if it isn't already configured
    * 如果尚未配置参数,请设置参数
    * */
  def setIfMissing(key: String, value: String): SparkConf = {
    //putIfAbsent如果key-value已经存在,则返回那个value,如果调用时map
    //没有找到key的mapping,返回一个null值
    //如果没有放弃putIfAbsent
    if (settings.putIfAbsent(key, value) == null) {
      logDeprecationWarning(key)
    }
    this
  }

  /**
   * Use Kryo serialization and register the given set of classes with Kryo.
   * If called multiple times, this will append the classes from all calls together.
    * 如果多次调用,这将从所有调用中附加类
   */
  def registerKryoClasses(classes: Array[Class[_]]): SparkConf = {
    val allClassNames = new LinkedHashSet[String]()
    allClassNames ++= get("spark.kryo.classesToRegister", "").split(',').filter(!_.isEmpty)
    allClassNames ++= classes.map(_.getName)

    set("spark.kryo.classesToRegister", allClassNames.mkString(","))
    set("spark.serializer", classOf[KryoSerializer].getName)
    this
  }

  private final val avroNamespace = "avro.schema."

  /**
   * Use Kryo serialization and register the given set of Avro schemas so that the generic
   * record serializer can decrease network IO
    * 使用Kryo序列化并注册给定的Avro模式集,以便通用记录序列化程序可以减少网络IO
   */
  def registerAvroSchemas(schemas: Schema*): SparkConf = {
    for (schema <- schemas) {
      set(avroNamespace + SchemaNormalization.parsingFingerprint64(schema), schema.toString)
    }
    this
  }

  /** Gets all the avro schemas in the configuration used in the generic Avro record serializer
    * 获取通用Avro记录序列化程序中使用的配置中的所有avro模式
    *  */
  def getAvroSchema: Map[Long, String] = {
    getAll.filter { case (k, v) => k.startsWith(avroNamespace) }
      .map { case (k, v) => (k.substring(avroNamespace.length).toLong, v) }
      .toMap
  }

  /**
    * Remove a parameter from the configuration
    * 从配置中删除参数
    *  */
  def remove(key: String): SparkConf = {
    settings.remove(key)
    this
  }

  /**
    * Get a parameter; throws a NoSuchElementException if it's not set
    * 获取参数, 如果未设置,则抛出NoSuchElementException异常
    *  */
  def get(key: String): String = {
    getOption(key).getOrElse(throw new NoSuchElementException(key))
  }

  /**
    * Get a parameter, falling back to a default if not set
    * 获取参数,如果未设置,则返回默认值
    * */
  def get(key: String, defaultValue: String): String = {
    getOption(key).getOrElse(defaultValue)
  }

  /**
   * Get a time parameter as seconds; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then seconds are assumed.
    * 以秒为单位获取时间参数,如果未设置,则抛出NoSuchElementException异常,如果不提供后缀,然后假定为秒。
   * @throws NoSuchElementException
   */
  def getTimeAsSeconds(key: String): Long = {
    Utils.timeStringAsSeconds(get(key))
  }

  /**
   * Get a time parameter as seconds, falling back to a default if not set. If no
   * suffix is provided then seconds are assumed.
    * 以秒为单位获取时间参数,如果未设置则返回到默认值,如果不提供后缀然后假定为秒。
   */
  def getTimeAsSeconds(key: String, defaultValue: String): Long = {
    Utils.timeStringAsSeconds(get(key, defaultValue))
  }

  /**
   * Get a time parameter as milliseconds; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then milliseconds are assumed.
    * 获取时间参数为毫秒,如果未设置,则抛出NoSuchElementException异常。 如果没有提供后缀，则假定为毫秒。
   * @throws NoSuchElementException
   */
  def getTimeAsMs(key: String): Long = {
    Utils.timeStringAsMs(get(key))
  }

  /**
   * Get a time parameter as milliseconds, falling back to a default if not set. If no
   * suffix is provided then milliseconds are assumed.
    * 获取时间参数为毫秒，如果未设置则返回到默认值。 如果没有提供后缀，则假定为毫秒。
   */
  def getTimeAsMs(key: String, defaultValue: String): Long = {
    Utils.timeStringAsMs(get(key, defaultValue))
  }

  /**
   * Get a size parameter as bytes; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then bytes are assumed.
    * 获取一个size参数作为字节;如果未设置,则抛出NoSuchElementException异常。如果没有提供后缀,则假定字节
   * @throws NoSuchElementException
   */
  def getSizeAsBytes(key: String): Long = {
    Utils.byteStringAsBytes(get(key))
  }

  /**
   * Get a size parameter as bytes, falling back to a default if not set. If no
   * suffix is provided then bytes are assumed.
    * 获取大小参数作为字节,如果未设置则返回到默认值。如果没有提供后缀,则假定字节。
   */
  def getSizeAsBytes(key: String, defaultValue: String): Long = {
    Utils.byteStringAsBytes(get(key, defaultValue))
  }

  /**
   * Get a size parameter as bytes, falling back to a default if not set.
    * 获取大小参数作为字节,如果未设置则返回到默认值。
   */
  def getSizeAsBytes(key: String, defaultValue: Long): Long = {
    Utils.byteStringAsBytes(get(key, defaultValue + "B"))
  }

  /**
   * Get a size parameter as Kibibytes; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then Kibibytes are assumed.
    * 获取大小参数为Kibibytes; 如果未设置,则抛出NoSuchElementException异常,如果没有提供后缀,则假定Kibibytes。
   * @throws NoSuchElementException
   */
  def getSizeAsKb(key: String): Long = {
    Utils.byteStringAsKb(get(key))
  }

  /**
   * Get a size parameter as Kibibytes, falling back to a default if not set. If no
   * suffix is provided then Kibibytes are assumed.
    * 获取大小参数为Kibibytes,如果未设置,则返回到默认值。如果没有提供后缀,则假定Kibibytes
   */
  def getSizeAsKb(key: String, defaultValue: String): Long = {
    Utils.byteStringAsKb(get(key, defaultValue))
  }

  /**
   * Get a size parameter as Mebibytes; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then Mebibytes are assumed.
    * 获取大小参数为Mebibytes; 如果未设置,则抛出NoSuchElementException异常,如果没有提供后缀,则假定Mebibytes
   * @throws NoSuchElementException
   */
  def getSizeAsMb(key: String): Long = {
    Utils.byteStringAsMb(get(key))
  }

  /**
   * Get a size parameter as Mebibytes, falling back to a default if not set. If no
   * suffix is provided then Mebibytes are assumed.
    * 获取大小参数为Mebibytes,如果未设置,则返回默认值,如果没有提供后缀,则假定Mebibytes。
   */
  def getSizeAsMb(key: String, defaultValue: String): Long = {
    Utils.byteStringAsMb(get(key, defaultValue))
  }

  /**
   * Get a size parameter as Gibibytes; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then Gibibytes are assumed.
    * 获取大小参数为Gibibytes;如果未设置,则抛出NoSuchElementException异常,如果没有提供后缀,则假定Gibibytes,
   * @throws NoSuchElementException
   */
  def getSizeAsGb(key: String): Long = {
    Utils.byteStringAsGb(get(key))
  }

  /**
   * Get a size parameter as Gibibytes, falling back to a default if not set. If no
   * suffix is provided then Gibibytes are assumed.
    * 获取大小参数为Gibibytes,如果未设置则返回到默认值,如果没有提供后缀,则假定Gibibytes
   */
  def getSizeAsGb(key: String, defaultValue: String): Long = {
    Utils.byteStringAsGb(get(key, defaultValue))
  }

  /**
    * Get a parameter as an Option
    * 获取参数作为选项
    * */
  def getOption(key: String): Option[String] = {
    Option(settings.get(key)).orElse(getDeprecatedConfig(key, this))
  }

  /**
    * Get all parameters as a list of pairs
    * 获取所有参数作为对(元组)的列表(list),Map转换list
    * */
  def getAll: Array[(String, String)] = {
    settings.entrySet().asScala.map(x => (x.getKey, x.getValue)).toArray
  }

  /**
    *  Get a parameter as an integer, falling back to a default if not set
    *  获取参数作为整数,如果未设置则返回默认值
    * */
  def getInt(key: String, defaultValue: Int): Int = {
    getOption(key).map(_.toInt).getOrElse(defaultValue)
  }

  /**
    * Get a parameter as a long, falling back to a default if not set
    * 获取参数long,如果未设置,则返回默认值
    * */
  def getLong(key: String, defaultValue: Long): Long = {
    getOption(key).map(_.toLong).getOrElse(defaultValue)
  }

  /** Get a parameter as a double, falling back to a default if not set
    * 获取参数作为double,如果未设置,则返回到默认值 */
  def getDouble(key: String, defaultValue: Double): Double = {
    getOption(key).map(_.toDouble).getOrElse(defaultValue)
  }

  /** Get a parameter as a boolean, falling back to a default if not set
    * 获取参数作为布尔值,如果未设置则返回默认值*/
  def getBoolean(key: String, defaultValue: Boolean): Boolean = {
    getOption(key).map(_.toBoolean).getOrElse(defaultValue)
  }

  /**
    * Get all executor environment variables set on this SparkConf
    * 获取在此SparkConf上设置的所有执行程序环境变量
    *  */
  def getExecutorEnv: Seq[(String, String)] = {
    val prefix = "spark.executorEnv."
    getAll.filter{case (k, v) => k.startsWith(prefix)}
          .map{case (k, v) => (k.substring(prefix.length), v)}
  }

  /** Get all akka conf variables set on this SparkConf
    * 获取在此SparkConf上设置的所有akka conf变量
    *  */
  def getAkkaConf: Seq[(String, String)] =
    /* This is currently undocumented. If we want to make this public we should consider
     * nesting options under the spark namespace to avoid conflicts with user akka options.
     * Otherwise users configuring their own akka code via system properties could mess up
     * spark's akka options.
     *
     * 这是目前无证的,如果我们想让这个公开，我们应该考虑在spark命名空间下嵌套选项,以避免与用户akka选项冲突。
     * 否则,通过系统属性配置自己的akka代码的用户可能会弄乱spark的akka选项
     *
     *   E.g. spark.akka.option.x.y.x = "value"
     */
    getAll.filter { case (k, _) => isAkkaConf(k) }

  /**
   * Returns the Spark application id, valid in the Driver after TaskScheduler registration and
   * from the start in the Executor.
    * 返回Spark应用程序标识，在TaskScheduler注册后的驱动程序中有效从执行开始
   */
  def getAppId: String = get("spark.app.id")

  /**
    *  Does the configuration contain a given parameter?
    *  配置是否包含给定的参数？
    * */
  def contains(key: String): Boolean = settings.containsKey(key)

  /** Copy this object
    * 复制此对象 */
  override def clone: SparkConf = {
    new SparkConf(false).setAll(getAll)
  }

  /**
   * By using this instead of System.getenv(), environment variables can be mocked
   * in unit tests.
    * 通过使用它而不是System.getenv(),环境变量可以被单位测试
   */
  private[spark] def getenv(name: String): String = System.getenv(name)

  /** Checks for illegal or deprecated config settings. Throws an exception for the former. Not
    * idempotent - may mutate this conf object to convert deprecated settings to supported ones.
    * 检查非法或不建议使用的配置设置,抛出前者的异常,不是幂等的-可能会突变此con对象,将不建议使用的设置转换为支持的设置,
    * 对Spark各种信息进行校验
    * */
  private[spark] def validateSettings() {
    if (contains("spark.local.dir")) {//用于暂存空间的目录,该目录用于保存map输出文件或者转储RDD
      val msg = "In Spark 1.0 and later spark.local.dir will be overridden by the value set by " +
        "the cluster manager (via SPARK_LOCAL_DIRS in mesos/standalone and LOCAL_DIRS in YARN)."
      logWarning(msg)
    }
  //传递给executor的额外JVM 选项,但是不能使用它来设置Spark属性或堆空间大小
    val executorOptsKey = "spark.executor.extraJavaOptions"
    //追加到executor类路径中的附加类路径
    val executorClasspathKey = "spark.executor.extraClassPath"
    
    val driverOptsKey = "spark.driver.extraJavaOptions"
    val driverClassPathKey = "spark.driver.extraClassPath"
    val driverLibraryPathKey = "spark.driver.extraLibraryPath"
    val sparkExecutorInstances = "spark.executor.instances"

    // Used by Yarn in 1.1 and before
    //在1.1及以前由Yarn使用
    //System.getenv()和System.getProperties()的区别
    //System.getenv() 返回系统环境变量值 设置系统环境变量：当前登录用户主目录下的".bashrc"文件中可以设置系统环境变量
    //System.getProperties() 返回Java进程变量值 通过命令行参数的"-D"选项
    sys.props.get("spark.driver.libraryPath").foreach { value =>
      val warning =
        s"""
          |spark.driver.libraryPath was detected (set to '$value').
          |This is deprecated in Spark 1.2+.
          |
          |Please instead use: $driverLibraryPathKey
        """.stripMargin
      logWarning(warning)
    }

    // Validate spark.executor.extraJavaOptions
    //验证spark.executor.extraJavaOptions
    getOption(executorOptsKey).map { javaOpts =>
      if (javaOpts.contains("-Dspark")) {
        val msg = s"$executorOptsKey is not allowed to set Spark options (was '$javaOpts'). " +
          "Set them directly on a SparkConf or in a properties file when using ./bin/spark-submit."
        throw new Exception(msg)
      }
      if (javaOpts.contains("-Xmx") || javaOpts.contains("-Xms")) {
        val msg = s"$executorOptsKey is not allowed to alter memory settings (was '$javaOpts'). " +
          "Use spark.executor.memory instead."
        throw new Exception(msg)
      }
    }

    // Validate memory fractions
    //验证内存分数
    val memoryKeys = Seq(
      "spark.storage.memoryFraction",//Java堆用于cache的比例
      "spark.shuffle.memoryFraction",
      "spark.shuffle.safetyFraction",
      "spark.storage.unrollFraction",
      "spark.storage.safetyFraction")
    for (key <- memoryKeys) {
      val value = getDouble(key, 0.5)
      if (value > 1 || value < 0) {
        throw new IllegalArgumentException("$key should be between 0 and 1 (was '$value').")
      }
    }

    // Check for legacy configs
    //检查旧配置
    //System.getenv()和System.getProperties()的区别
    //System.getenv() 返回系统环境变量值 设置系统环境变量：当前登录用户主目录下的".bashrc"文件中可以设置系统环境变量
    //System.getProperties() 返回Java进程变量值 通过命令行参数的"-D"选项
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
          | - SPARK_DAEMON_JAVA_OPTS to set java options for standalone daemons (master or worker)
        """.stripMargin
      logWarning(warning)

      for (key <- Seq(executorOptsKey, driverOptsKey)) {
        if (getOption(key).isDefined) {//查询Key已经被使用
          throw new SparkException(s"Found both $key and SPARK_JAVA_OPTS. Use only the former.")
        } else {
          logWarning(s"Setting '$key' to '$value' as a work-around.")
          set(key, value)
        }
      }
    }
    //System.getenv()和System.getProperties()的区别
    //System.getenv() 返回系统环境变量值 设置系统环境变量：当前登录用户主目录下的".bashrc"文件中可以设置系统环境变量
    //System.getProperties() 返回Java进程变量值 通过命令行参数的"-D"选项
    sys.env.get("SPARK_CLASSPATH").foreach { value =>
      val warning =
        s"""
          |SPARK_CLASSPATH was detected (set to '$value').
          |This is deprecated in Spark 1.0+.
          |
          |Please instead use:
          | - ./spark-submit with --driver-class-path to augment the driver classpath
          | - spark.executor.extraClassPath to augment the executor classpath
        """.stripMargin
      logWarning(warning)

      for (key <- Seq(executorClasspathKey, driverClassPathKey)) {
        if (getOption(key).isDefined) {
          throw new SparkException(s"Found both $key and SPARK_CLASSPATH. Use only the former.")
        } else {
          logWarning(s"Setting '$key' to '$value' as a work-around.")
          set(key, value)
        }
      }
    }

    if (!contains(sparkExecutorInstances)) {
    //每个slave机器上启动的worker实例个数（默认：1）
      //System.getenv()和System.getProperties()的区别
      //System.getenv() 返回系统环境变量值 设置系统环境变量：当前登录用户主目录下的".bashrc"文件中可以设置系统环境变量
      //System.getProperties() 返回Java进程变量值 通过命令行参数的"-D"选项
      sys.env.get("SPARK_WORKER_INSTANCES").foreach { value =>
        val warning =
          s"""
             |SPARK_WORKER_INSTANCES was detected (set to '$value').
             |This is deprecated in Spark 1.0+.
             |
             |Please instead use:
             | - ./spark-submit with --num-executors to specify the number of executors
             | - Or set SPARK_EXECUTOR_INSTANCES
             | - spark.executor.instances to configure the number of instances in the spark config.
        """.stripMargin
        logWarning(warning)

        set("spark.executor.instances", value)
      }
    }
  }

  /**
   * Return a string listing all keys and values, one per line. This is useful to print the
   * configuration out for debugging.
    * 返回一个列出所有键和值的字符串,每行一个。 这对打印配置进行调试非常有用
   */
  def toDebugString: String = {
    getAll.sorted.map{case (k, v) => k + "=" + v}.mkString("\n")
  }

}

private[spark] object SparkConf extends Logging {

  /**
   * Maps deprecated config keys to information about the deprecation.
    * 将不推荐使用的配置键映射到有关弃用的信息
   *
   * The extra information is logged as a warning when the config is present in the user's
   * configuration.
    * 当配置存在于用户配置中时,额外的信息将被记录为警告
   */
  private val deprecatedConfigs: Map[String, DeprecatedConfig] = {
    val configs = Seq(
      DeprecatedConfig("spark.cache.class", "0.8",
        "The spark.cache.class property is no longer being used! Specify storage levels using " +
        "the RDD.persist() method instead."),
      DeprecatedConfig("spark.yarn.user.classpath.first", "1.3",
        "Please use spark.{driver,executor}.userClassPathFirst instead."),
      DeprecatedConfig("spark.kryoserializer.buffer.mb", "1.4",
        "Please use spark.kryoserializer.buffer instead. The default value for " +
          "spark.kryoserializer.buffer.mb was previously specified as '0.064'. Fractional values " +
          "are no longer accepted. To specify the equivalent now, one may use '64k'.")
    )

    Map(configs.map { cfg => (cfg.key -> cfg) } : _*)
  }

  /**
   * Maps a current config key to alternate keys that were used in previous version of Spark.
    * 将当前配置密钥映射到先前版本的Spark中使用的备用密钥
   *
   * The alternates are used in the order defined in this map. If deprecated configs are
   * present in the user's configuration, a warning is logged.
    * 替代品按照该Map中定义的顺序使用,如果用户配置中存在不建议使用的配置,则会记录一个警告。
   */
  private val configsWithAlternatives = Map[String, Seq[AlternateConfig]](
  //executor在加载类的时候是否优先使用用户自定义的JAR包,而不是Spark带有的JAR包,目前,该属性只是一项试验功能
    "spark.executor.userClassPathFirst" -> Seq(
    //executor在加载类的时候是否优先使用用户自定义的JAR包,而不是Spark带有的JAR包
      AlternateConfig("spark.files.userClassPathFirst", "1.3")),
    "spark.history.fs.update.interval" -> Seq(
      AlternateConfig("spark.history.fs.update.interval.seconds", "1.4"),
      AlternateConfig("spark.history.fs.updateInterval", "1.3"),
      AlternateConfig("spark.history.updateInterval", "1.3")),
    "spark.history.fs.cleaner.interval" -> Seq(
      AlternateConfig("spark.history.fs.cleaner.interval.seconds", "1.4")),
    "spark.history.fs.cleaner.maxAge" -> Seq(
      AlternateConfig("spark.history.fs.cleaner.maxAge.seconds", "1.4")),
    "spark.yarn.am.waitTime" -> Seq(
      AlternateConfig("spark.yarn.applicationMaster.waitTries", "1.3",
        // Translate old value to a duration, with 10s wait time per try.
        //将旧值转换为持续时间,每次尝试10秒等待时间
        translation = s => s"${s.toLong * 10}s")),
    "spark.reducer.maxSizeInFlight" -> Seq(   
    //在Shuffle的时候,每个Reducer任务获取缓存数据指定大小(以兆字节为单位)  
      AlternateConfig("spark.reducer.maxMbInFlight", "1.4")),
    "spark.kryoserializer.buffer" ->
        Seq(AlternateConfig("spark.kryoserializer.buffer.mb", "1.4",
          translation = s => s"${(s.toDouble * 1000).toInt}k")),
    "spark.kryoserializer.buffer.max" -> Seq(
      AlternateConfig("spark.kryoserializer.buffer.max.mb", "1.4")),
    "spark.shuffle.file.buffer" -> Seq(
    //每个shuffle的文件输出流内存缓冲区的大小,以KB为单位.
      AlternateConfig("spark.shuffle.file.buffer.kb", "1.4")),
    "spark.executor.logs.rolling.maxSize" -> Seq(
      AlternateConfig("spark.executor.logs.rolling.size.maxBytes", "1.4")),
    "spark.io.compression.snappy.blockSize" -> Seq(
      AlternateConfig("spark.io.compression.snappy.block.size", "1.4")),
    "spark.io.compression.lz4.blockSize" -> Seq(
      AlternateConfig("spark.io.compression.lz4.block.size", "1.4")),
    "spark.rpc.numRetries" -> Seq(
      AlternateConfig("spark.akka.num.retries", "1.4")),
    "spark.rpc.retry.wait" -> Seq(
      AlternateConfig("spark.akka.retry.wait", "1.4")),
    "spark.rpc.askTimeout" -> Seq(
      AlternateConfig("spark.akka.askTimeout", "1.4")),
    "spark.rpc.lookupTimeout" -> Seq(
      AlternateConfig("spark.akka.lookupTimeout", "1.4")),
    "spark.streaming.fileStream.minRememberDuration" -> Seq(
      AlternateConfig("spark.streaming.minRememberDuration", "1.5"))
    )

  /**
   * A view of `configsWithAlternatives` that makes it more efficient to look up deprecated
   * config keys.
    * “configsWithAlternatives”的视图,可以更有效地查找已弃用的配置密钥
   *
   * Maps the deprecated config name to a 2-tuple (new config name, alternate config info).
    * 将不推荐的配置名称映射到2元组(新配置名称,备用配置信息)
   */
  private val allAlternatives: Map[String, (String, AlternateConfig)] = {
    configsWithAlternatives.keys.flatMap { key =>
      configsWithAlternatives(key).map { cfg => (cfg.key -> (key -> cfg)) }
    }.toMap
  }

  /**
   * Return whether the given config is an akka config (e.g. akka.actor.provider).
    * 返回给定的配置是否是akka配置(例如akka.actor.provider)
   * Note that this does not include spark-specific akka configs (e.g. spark.akka.timeout).
    * 请注意,这不包括Spark特定的akka配置(例如spark.akka.timeout)
   */
  def isAkkaConf(name: String): Boolean = name.startsWith("akka.")

  /**
   * Return whether the given config should be passed to an executor on start-up.
   * 返回给定的配置是否应在启动时传递给执行程序
   * Certain akka and authentication configs are required from the executor when it connects to
   * the scheduler, while the rest of the spark configs can be inherited from the driver later.
    * 当执行器连接到调度程序时,需要某些akka和身份验证配置,而稍后可以从驱动程序继承其余的spark配置
   */
  def isExecutorStartupConf(name: String): Boolean = {
    isAkkaConf(name) ||
    name.startsWith("spark.akka") ||
    (name.startsWith("spark.auth") && name != SecurityManager.SPARK_AUTH_SECRET_CONF) ||
    name.startsWith("spark.ssl") ||
    isSparkPortConf(name)
  }

  /**
   * Return true if the given config matches either `spark.*.port` or `spark.port.*`.
    * 如果给定的配置匹配`spark。*。port`或`spark.port。*`,则返回true
   */
  def isSparkPortConf(name: String): Boolean = {
    (name.startsWith("spark.") && name.endsWith(".port")) || name.startsWith("spark.port.")
  }

  /**
   * Looks for available deprecated keys for the given config option, and return the first
   * value available.
    * 查找给定配置选项的可用不推荐的key,然后返回第一个值可用
   */
  def getDeprecatedConfig(key: String, conf: SparkConf): Option[String] = {
    configsWithAlternatives.get(key).flatMap { alts =>
      alts.collectFirst { case alt if conf.contains(alt.key) =>
        val value = conf.get(alt.key)
        if (alt.translation != null) alt.translation(value) else value
      }
    }
  }

  /**
   * Logs a warning message if the given config key is deprecated.
    * 如果不适用给定的配置key,则会记录一条警告消息
   */
  def logDeprecationWarning(key: String): Unit = {
    //不赞成配制属性
    deprecatedConfigs.get(key).foreach { cfg =>
      logWarning(
        s"The configuration key '$key' has been deprecated as of Spark ${cfg.version} and " +
        s"may be removed in the future. ${cfg.deprecationMessage}")
    }
    //替代配制属性
    allAlternatives.get(key).foreach { case (newKey, cfg) =>
      logWarning(
        s"The configuration key '$key' has been deprecated as of Spark ${cfg.version} and " +
        s"and may be removed in the future. Please use the new key '$newKey' instead.")
    }
  }

  /**
   * Holds information about keys that have been deprecated and do not have a replacement.
    * 保存有关已被弃用且没有替换键的信息
   *
   * @param key The deprecated key. 已弃用的密钥
   * @param version Version of Spark where key was deprecated. 已禁用密钥的Spark版本
   * @param deprecationMessage Message to include in the deprecation warning.
   */
  private case class DeprecatedConfig(
      key: String,
      version: String,
      deprecationMessage: String)

  /**
   * Information about an alternate configuration key that has been deprecated.
    * 有关已弃用的备用配置key的信息
   *
   * @param key The deprecated config key.
   * @param version The Spark version in which the key was deprecated.
   * @param translation A translation function for converting old config values into new ones.
   */
  private case class AlternateConfig(
      key: String,
      version: String,
      translation: String => String = null)

}
