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

package org.apache.spark.api.java

import java.io.Closeable
import java.util
import java.util.{Map => JMap}

import scala.collection.JavaConversions
import scala.collection.JavaConversions._
import scala.language.implicitConversions
import scala.reflect.ClassTag

import com.google.common.base.Optional
import org.apache.hadoop.conf.Configuration
import org.apache.spark.input.PortableDataStream
import org.apache.hadoop.mapred.{InputFormat, JobConf}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}

import org.apache.spark._
import org.apache.spark.AccumulatorParam._
import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.{EmptyRDD, HadoopRDD, NewHadoopRDD, RDD}

/**
 * A Java-friendly version of [[org.apache.spark.SparkContext]] that returns
 * [[org.apache.spark.api.java.JavaRDD]]s and works with Java collections instead of Scala ones.
 *
 * Only one SparkContext may be active per JVM.  You must `stop()` the active SparkContext before
 * creating a new one.  This limitation may eventually be removed; see SPARK-2243 for more details.
 */
class JavaSparkContext(val sc: SparkContext)
  extends JavaSparkContextVarargsWorkaround with Closeable {

  /**
   * Create a JavaSparkContext that loads settings from system properties (for instance, when
   * launching with ./bin/spark-submit).
    * 创建一个JavaSparkContext,用于从系统属性加载设置(例如，使用./bin/spark-submit启动时)
   */
  def this() = this(new SparkContext())

  /**
   * @param conf a [[org.apache.spark.SparkConf]] object specifying Spark parameters
   */
  def this(conf: SparkConf) = this(new SparkContext(conf))

  /**
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI
   */
  def this(master: String, appName: String) = this(new SparkContext(master, appName))

  /**
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI
   * @param conf a [[org.apache.spark.SparkConf]] object specifying other Spark parameters
   */
  def this(master: String, appName: String, conf: SparkConf) =
    this(conf.setMaster(master).setAppName(appName))

  /**
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI
   * @param sparkHome The SPARK_HOME directory on the slave nodes
   * @param jarFile JAR file to send to the cluster. This can be a path on the local file system
   *                or an HDFS, HTTP, HTTPS, or FTP URL.
   */
  def this(master: String, appName: String, sparkHome: String, jarFile: String) =
    this(new SparkContext(master, appName, sparkHome, Seq(jarFile)))

  /**
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI
   * @param sparkHome The SPARK_HOME directory on the slave nodes
   * @param jars Collection of JARs to send to the cluster. These can be paths on the local file
   *             system or HDFS, HTTP, HTTPS, or FTP URLs.
   */
  def this(master: String, appName: String, sparkHome: String, jars: Array[String]) =
    this(new SparkContext(master, appName, sparkHome, jars.toSeq))

  /**
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI
   * @param sparkHome The SPARK_HOME directory on the slave nodes
   * @param jars Collection of JARs to send to the cluster. These can be paths on the local file
   *             system or HDFS, HTTP, HTTPS, or FTP URLs.
   * @param environment Environment variables to set on worker nodes
   */
  def this(master: String, appName: String, sparkHome: String, jars: Array[String],
      environment: JMap[String, String]) =
    this(new SparkContext(master, appName, sparkHome, jars.toSeq, environment, Map()))

  private[spark] val env = sc.env

  def statusTracker: JavaSparkStatusTracker = new JavaSparkStatusTracker(sc)

  def isLocal: java.lang.Boolean = sc.isLocal

  def sparkUser: String = sc.sparkUser

  def master: String = sc.master

  def appName: String = sc.appName

  def jars: util.List[String] = sc.jars

  def startTime: java.lang.Long = sc.startTime

  /** The version of Spark on which this application is running.
    * 运行此应用程序的Spark版本*/
  def version: String = sc.version

  /** Default level of parallelism to use when not given by user (e.g. parallelize and makeRDD).
    * 未由用户给出时使用的默认并行级别（例如parallelize和makeRDD）*/
  def defaultParallelism: java.lang.Integer = sc.defaultParallelism

  /**
   * Default min number of partitions for Hadoop RDDs when not given by user.
   * @deprecated As of Spark 1.0.0, defaultMinSplits is deprecated, use
   *            {@link #defaultMinPartitions()} instead
   */
  @deprecated("use defaultMinPartitions", "1.0.0")
  def defaultMinSplits: java.lang.Integer = sc.defaultMinSplits

  /** Default min number of partitions for Hadoop RDDs when not given by user
    * 当用户未提供时,Hadoop RDD的默认最小分区数*/
  def defaultMinPartitions: java.lang.Integer = sc.defaultMinPartitions

  /** Distribute a local Scala collection to form an RDD.
    * 分发本地Scala集合以形成RDD*/
  def parallelize[T](list: java.util.List[T], numSlices: Int): JavaRDD[T] = {
    implicit val ctag: ClassTag[T] = fakeClassTag
    sc.parallelize(JavaConversions.asScalaBuffer(list), numSlices)
  }

  /** Get an RDD that has no partitions or elements.
    * 获取没有分区或元素的RDD*/
  def emptyRDD[T]: JavaRDD[T] = {
    implicit val ctag: ClassTag[T] = fakeClassTag
    JavaRDD.fromRDD(new EmptyRDD[T](sc))
  }


  /** Distribute a local Scala collection to form an RDD.
    * 分发本地Scala集合以形成RDD*/
  def parallelize[T](list: java.util.List[T]): JavaRDD[T] =
    parallelize(list, sc.defaultParallelism)

  /** Distribute a local Scala collection to form an RDD.
    * 分发本地Scala集合以形成RDD*/
  def parallelizePairs[K, V](list: java.util.List[Tuple2[K, V]], numSlices: Int)
  : JavaPairRDD[K, V] = {
    implicit val ctagK: ClassTag[K] = fakeClassTag
    implicit val ctagV: ClassTag[V] = fakeClassTag
    JavaPairRDD.fromRDD(sc.parallelize(JavaConversions.asScalaBuffer(list), numSlices))
  }

  /** Distribute a local Scala collection to form an RDD.
    * 分发本地Scala集合以形成RDD*/
  def parallelizePairs[K, V](list: java.util.List[Tuple2[K, V]]): JavaPairRDD[K, V] =
    parallelizePairs(list, sc.defaultParallelism)

  /** Distribute a local Scala collection to form an RDD.
    * 分发本地Scala集合以形成RDD*/
  def parallelizeDoubles(list: java.util.List[java.lang.Double], numSlices: Int): JavaDoubleRDD =
    JavaDoubleRDD.fromRDD(sc.parallelize(JavaConversions.asScalaBuffer(list).map(_.doubleValue()),
      numSlices))

  /** Distribute a local Scala collection to form an RDD.
    * 分发本地Scala集合以形成RDD*/
  def parallelizeDoubles(list: java.util.List[java.lang.Double]): JavaDoubleRDD =
    parallelizeDoubles(list, sc.defaultParallelism)

  /**
   * Read a text file from HDFS, a local file system (available on all nodes), or any
   * Hadoop-supported file system URI, and return it as an RDD of Strings.
    * 从HDFS读取文本文件,本地文件系统(在所有节点上都可用)或任何Hadoop支持的文件系统URI,并将其作为字符串的RDD返回。
   */
  def textFile(path: String): JavaRDD[String] = sc.textFile(path)

  /**
   * Read a text file from HDFS, a local file system (available on all nodes), or any
   * Hadoop-supported file system URI, and return it as an RDD of Strings.
    * 从HDFS读取文本文件,本地文件系统(在所有节点上都可用)
    * 或任何Hadoop支持的文件系统URI,并将其作为字符串的RDD返回。
   */
  def textFile(path: String, minPartitions: Int): JavaRDD[String] =
    sc.textFile(path, minPartitions)



  /**
   * Read a directory of text files from HDFS, a local file system (available on all nodes), or any
   * Hadoop-supported file system URI. Each file is read as a single record and returned in a
   * key-value pair, where the key is the path of each file, the value is the content of each file.
    *
   * 从HDFS读取文本文件目录,本地文件系统（在所有节点上都可用）或任何支持Hadoop的文件系统URI,
    * 每个文件都作为单个记录读取并以键值对的形式返回,其中键是每个文件的路径,值是每个文件的内容。
   * <p> For example, if you have the following files:
   * {{{
   *   hdfs://a-hdfs-path/part-00000
   *   hdfs://a-hdfs-path/part-00001
   *   ...
   *   hdfs://a-hdfs-path/part-nnnnn
   * }}}
   *
   * Do
   * {{{
   *   JavaPairRDD<String, String> rdd = sparkContext.wholeTextFiles("hdfs://a-hdfs-path")
   * }}}
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
   *
   * @param minPartitions A suggestion value of the minimal splitting number for input data.
   */
  def wholeTextFiles(path: String, minPartitions: Int): JavaPairRDD[String, String] =
    new JavaPairRDD(sc.wholeTextFiles(path, minPartitions))

  /**
   * Read a directory of text files from HDFS, a local file system (available on all nodes), or any
   * Hadoop-supported file system URI. Each file is read as a single record and returned in a
   * key-value pair, where the key is the path of each file, the value is the content of each file.
    *
    * 从HDFS读取文本文件目录,本地文件系统（在所有节点上都可用）或任何支持Hadoop的文件系统URI,
    * 每个文件都作为单个记录读取并以键值对的形式返回,其中键是每个文件的路径,值是每个文件的内容。
   *
   * @see `wholeTextFiles(path: String, minPartitions: Int)`.
   */
  def wholeTextFiles(path: String): JavaPairRDD[String, String] =
    new JavaPairRDD(sc.wholeTextFiles(path))

  /**
   * Read a directory of binary files from HDFS, a local file system (available on all nodes),
   * or any Hadoop-supported file system URI as a byte array. Each file is read as a single
   * record and returned in a key-value pair, where the key is the path of each file,
   * the value is the content of each file.
    *
    * 从HDFS,本地文件系统(在所有节点上可用)或任何Hadoop支持的文件系统URI作为字节数组读取二进制文件的目录,
    * 每个文件都作为单个记录读取并以键值对的形式返回,其中键是每个文件的路径,值是每个文件的内容
   *
   * For example, if you have the following files:
   * {{{
   *   hdfs://a-hdfs-path/part-00000
   *   hdfs://a-hdfs-path/part-00001
   *   ...
   *   hdfs://a-hdfs-path/part-nnnnn
   * }}}
   *
   * Do
   * `JavaPairRDD<String, byte[]> rdd = sparkContext.dataStreamFiles("hdfs://a-hdfs-path")`,
   *
   * then `rdd` contains
   * {{{
   *   (a-hdfs-path/part-00000, its content)
   *   (a-hdfs-path/part-00001, its content)
   *   ...
   *   (a-hdfs-path/part-nnnnn, its content)
   * }}}
   *
   * @note Small files are preferred; very large files but may cause bad performance.
   *
   * @param minPartitions A suggestion value of the minimal splitting number for input data.
   */
  def binaryFiles(path: String, minPartitions: Int): JavaPairRDD[String, PortableDataStream] =
    new JavaPairRDD(sc.binaryFiles(path, minPartitions))

  /**
   * :: Experimental ::
   *
   * Read a directory of binary files from HDFS, a local file system (available on all nodes),
   * or any Hadoop-supported file system URI as a byte array. Each file is read as a single
   * record and returned in a key-value pair, where the key is the path of each file,
   * the value is the content of each file.
    *
    * 从HDFS,本地文件系统(在所有节点上可用)或任何Hadoop支持的文件系统URI作为字节数组读取二进制文件的目录,
    * 每个文件都作为单个记录读取并以键值对的形式返回,其中键是每个文件的路径,值是每个文件的内容。
   *
   * For example, if you have the following files:
   * {{{
   *   hdfs://a-hdfs-path/part-00000
   *   hdfs://a-hdfs-path/part-00001
   *   ...
   *   hdfs://a-hdfs-path/part-nnnnn
   * }}}
   *
   * Do
   * `JavaPairRDD<String, byte[]> rdd = sparkContext.dataStreamFiles("hdfs://a-hdfs-path")`,
   *
   * then `rdd` contains
   * {{{
   *   (a-hdfs-path/part-00000, its content)
   *   (a-hdfs-path/part-00001, its content)
   *   ...
   *   (a-hdfs-path/part-nnnnn, its content)
   * }}}
   *
   * @note Small files are preferred; very large files but may cause bad performance.
   */
  @Experimental
  def binaryFiles(path: String): JavaPairRDD[String, PortableDataStream] =
    new JavaPairRDD(sc.binaryFiles(path, defaultMinPartitions))

  /**
   * :: Experimental ::
   *
   * Load data from a flat binary file, assuming the length of each record is constant.
    * 假设每条记录的长度不变,从平面二进制文件加载数据。
   *
   * @param path Directory to the input data files
   * @return An RDD of data with values, represented as byte arrays
   */
  @Experimental
  def binaryRecords(path: String, recordLength: Int): JavaRDD[Array[Byte]] = {
    new JavaRDD(sc.binaryRecords(path, recordLength))
  }

  /** Get an RDD for a Hadoop SequenceFile with given key and value types.
    * 使用给定的键和值类型获取Hadoop SequenceFile的RDD
    *
    * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
    * record, directly caching the returned RDD will create many references to the same object.
    * If you plan to directly cache Hadoop writable objects, you should first copy them using
    * a `map` function.
    * */
  def sequenceFile[K, V](path: String,
    keyClass: Class[K],
    valueClass: Class[V],
    minPartitions: Int
    ): JavaPairRDD[K, V] = {
    implicit val ctagK: ClassTag[K] = ClassTag(keyClass)
    implicit val ctagV: ClassTag[V] = ClassTag(valueClass)
    new JavaPairRDD(sc.sequenceFile(path, keyClass, valueClass, minPartitions))
  }

  /** Get an RDD for a Hadoop SequenceFile.
    * 获取Hadoop SequenceFile的RDD
    *
    * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
    * record, directly caching the returned RDD will create many references to the same object.
    * If you plan to directly cache Hadoop writable objects, you should first copy them using
    * a `map` function.
    */
  def sequenceFile[K, V](path: String, keyClass: Class[K], valueClass: Class[V]):
  JavaPairRDD[K, V] = {
    implicit val ctagK: ClassTag[K] = ClassTag(keyClass)
    implicit val ctagV: ClassTag[V] = ClassTag(valueClass)
    new JavaPairRDD(sc.sequenceFile(path, keyClass, valueClass))
  }

  /**
   * Load an RDD saved as a SequenceFile containing serialized objects, with NullWritable keys and
   * BytesWritable values that contain a serialized partition. This is still an experimental storage
   * format and may not be supported exactly as is in future Spark releases. It will also be pretty
   * slow if you use the default serializer (Java serialization), though the nice thing about it is
   * that there's very little effort required to save arbitrary objects.
    * 加载保存为包含序列化对象的SequenceFile的RDD，其中包含NullWritable键和包含序列化分区的BytesWritable值,
    * 这仍然是一种实验性存储格式,可能不会像将来的Spark版本那样完全支持,
    * 如果使用默认的序列化程序(Java序列化),它也会非常慢,尽管它的好处在于保存任意对象所需的工作量很少。
   */
  def objectFile[T](path: String, minPartitions: Int): JavaRDD[T] = {
    implicit val ctag: ClassTag[T] = fakeClassTag
    sc.objectFile(path, minPartitions)(ctag)
  }

  /**
   * Load an RDD saved as a SequenceFile containing serialized objects, with NullWritable keys and
   * BytesWritable values that contain a serialized partition. This is still an experimental storage
   * format and may not be supported exactly as is in future Spark releases. It will also be pretty
   * slow if you use the default serializer (Java serialization), though the nice thing about it is
   * that there's very little effort required to save arbitrary objects.
    *
    * 加载保存为包含序列化对象的SequenceFile的RDD,其中包含NullWritable键和包含序列化分区的BytesWritable值,
    * 这仍然是一种实验性存储格式,可能不会像将来的Spark版本那样完全支持,
    * 如果使用默认的序列化程序（Java序列化）,它也会非常慢,尽管它的好处在于保存任意对象所需的工作量很少

    */
  def objectFile[T](path: String): JavaRDD[T] = {
    implicit val ctag: ClassTag[T] = fakeClassTag
    sc.objectFile(path)(ctag)
  }

  /**
   * Get an RDD for a Hadoop-readable dataset from a Hadooop JobConf giving its InputFormat and any
   * other necessary info (e.g. file name for a filesystem-based dataset, table name for HyperTable,
   * etc).
    *
    * 从Hadooop JobConf获取Hadoop可读数据集的RDD,提供其InputFormat和任何其他必要信息(例如,基于文件系统的数据集的文件名,HyperTable的表名等)
   *
   * @param conf JobConf for setting up the dataset. Note: This will be put into a Broadcast.
   *             Therefore if you plan to reuse this conf to create multiple RDDs, you need to make
   *             sure you won't modify the conf. A safe approach is always creating a new conf for
   *             a new RDD.
   * @param inputFormatClass Class of the InputFormat
   * @param keyClass Class of the keys
   * @param valueClass Class of the values
   * @param minPartitions Minimum number of Hadoop Splits to generate.
   *
   * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD will create many references to the same object.
   * If you plan to directly cache Hadoop writable objects, you should first copy them using
   * a `map` function.
   */
  def hadoopRDD[K, V, F <: InputFormat[K, V]](
    conf: JobConf,
    inputFormatClass: Class[F],
    keyClass: Class[K],
    valueClass: Class[V],
    minPartitions: Int
    ): JavaPairRDD[K, V] = {
    implicit val ctagK: ClassTag[K] = ClassTag(keyClass)
    implicit val ctagV: ClassTag[V] = ClassTag(valueClass)
    val rdd = sc.hadoopRDD(conf, inputFormatClass, keyClass, valueClass, minPartitions)
    new JavaHadoopRDD(rdd.asInstanceOf[HadoopRDD[K, V]])
  }

  /**
   * Get an RDD for a Hadoop-readable dataset from a Hadooop JobConf giving its InputFormat and any
   * other necessary info (e.g. file name for a filesystem-based dataset, table name for HyperTable,
    * 从Hadooop JobConf获取Hadoop可读数据集的RDD,提供其InputFormat和任何其他必要信息(例如,基于文件系统的数据集的文件名,HyperTable的表名)
   *
   * @param conf JobConf for setting up the dataset. Note: This will be put into a Broadcast.
   *             Therefore if you plan to reuse this conf to create multiple RDDs, you need to make
   *             sure you won't modify the conf. A safe approach is always creating a new conf for
   *             a new RDD.
   * @param inputFormatClass Class of the InputFormat
   * @param keyClass Class of the keys
   * @param valueClass Class of the values
   *
   * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD will create many references to the same object.
   * If you plan to directly cache Hadoop writable objects, you should first copy them using
   * a `map` function.
   */
  def hadoopRDD[K, V, F <: InputFormat[K, V]](
    conf: JobConf,
    inputFormatClass: Class[F],
    keyClass: Class[K],
    valueClass: Class[V]
    ): JavaPairRDD[K, V] = {
    implicit val ctagK: ClassTag[K] = ClassTag(keyClass)
    implicit val ctagV: ClassTag[V] = ClassTag(valueClass)
    val rdd = sc.hadoopRDD(conf, inputFormatClass, keyClass, valueClass)
    new JavaHadoopRDD(rdd.asInstanceOf[HadoopRDD[K, V]])
  }

  /** Get an RDD for a Hadoop file with an arbitrary InputFormat.
    * 获取具有任意InputFormat的Hadoop文件的RDD
    *
    * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
    * record, directly caching the returned RDD will create many references to the same object.
    * If you plan to directly cache Hadoop writable objects, you should first copy them using
    * a `map` function.
    */
  def hadoopFile[K, V, F <: InputFormat[K, V]](
    path: String,
    inputFormatClass: Class[F],
    keyClass: Class[K],
    valueClass: Class[V],
    minPartitions: Int
    ): JavaPairRDD[K, V] = {
    implicit val ctagK: ClassTag[K] = ClassTag(keyClass)
    implicit val ctagV: ClassTag[V] = ClassTag(valueClass)
    val rdd = sc.hadoopFile(path, inputFormatClass, keyClass, valueClass, minPartitions)
    new JavaHadoopRDD(rdd.asInstanceOf[HadoopRDD[K, V]])
  }

  /** Get an RDD for a Hadoop file with an arbitrary InputFormat
    * 获取具有任意InputFormat的Hadoop文件的RDD
    *
    * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
    * record, directly caching the returned RDD will create many references to the same object.
    * If you plan to directly cache Hadoop writable objects, you should first copy them using
    * a `map` function.
    */
  def hadoopFile[K, V, F <: InputFormat[K, V]](
    path: String,
    inputFormatClass: Class[F],
    keyClass: Class[K],
    valueClass: Class[V]
    ): JavaPairRDD[K, V] = {
    implicit val ctagK: ClassTag[K] = ClassTag(keyClass)
    implicit val ctagV: ClassTag[V] = ClassTag(valueClass)
    val rdd = sc.hadoopFile(path, inputFormatClass, keyClass, valueClass)
    new JavaHadoopRDD(rdd.asInstanceOf[HadoopRDD[K, V]])
  }

  /**
   * Get an RDD for a given Hadoop file with an arbitrary new API InputFormat
   * and extra configuration options to pass to the input format.
    * 获取给定Hadoop文件的RDD,其中包含任意新的API InputFormat和额外的配置选项以传递给输入格式
   *
   * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD will create many references to the same object.
   * If you plan to directly cache Hadoop writable objects, you should first copy them using
   * a `map` function.
   */
  def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]](
    path: String,
    fClass: Class[F],
    kClass: Class[K],
    vClass: Class[V],
    conf: Configuration): JavaPairRDD[K, V] = {
    implicit val ctagK: ClassTag[K] = ClassTag(kClass)
    implicit val ctagV: ClassTag[V] = ClassTag(vClass)
    val rdd = sc.newAPIHadoopFile(path, fClass, kClass, vClass, conf)
    new JavaNewHadoopRDD(rdd.asInstanceOf[NewHadoopRDD[K, V]])
  }

  /**
   * Get an RDD for a given Hadoop file with an arbitrary new API InputFormat
   * and extra configuration options to pass to the input format.
    * 获取给定Hadoop文件的RDD,其中包含任意新的API InputFormat和额外的配置选项以传递给输入格式。
   *
   * @param conf Configuration for setting up the dataset. Note: This will be put into a Broadcast.
   *             Therefore if you plan to reuse this conf to create multiple RDDs, you need to make
   *             sure you won't modify the conf. A safe approach is always creating a new conf for
   *             a new RDD.
   * @param fClass Class of the InputFormat
   * @param kClass Class of the keys
   * @param vClass Class of the values
   *
   * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD will create many references to the same object.
   * If you plan to directly cache Hadoop writable objects, you should first copy them using
   * a `map` function.
   */
  def newAPIHadoopRDD[K, V, F <: NewInputFormat[K, V]](
    conf: Configuration,
    fClass: Class[F],
    kClass: Class[K],
    vClass: Class[V]): JavaPairRDD[K, V] = {
    implicit val ctagK: ClassTag[K] = ClassTag(kClass)
    implicit val ctagV: ClassTag[V] = ClassTag(vClass)
    val rdd = sc.newAPIHadoopRDD(conf, fClass, kClass, vClass)
    new JavaNewHadoopRDD(rdd.asInstanceOf[NewHadoopRDD[K, V]])
  }

  /** Build the union of two or more RDDs. 构建两个或更多RDD的并集*/
  override def union[T](first: JavaRDD[T], rest: java.util.List[JavaRDD[T]]): JavaRDD[T] = {
    val rdds: Seq[RDD[T]] = (Seq(first) ++ asScalaBuffer(rest)).map(_.rdd)
    implicit val ctag: ClassTag[T] = first.classTag
    sc.union(rdds)
  }

  /** Build the union of two or more RDDs. 构建两个或更多RDD的并集*/
  override def union[K, V](first: JavaPairRDD[K, V], rest: java.util.List[JavaPairRDD[K, V]])
      : JavaPairRDD[K, V] = {
    val rdds: Seq[RDD[(K, V)]] = (Seq(first) ++ asScalaBuffer(rest)).map(_.rdd)
    implicit val ctag: ClassTag[(K, V)] = first.classTag
    implicit val ctagK: ClassTag[K] = first.kClassTag
    implicit val ctagV: ClassTag[V] = first.vClassTag
    new JavaPairRDD(sc.union(rdds))
  }

  /** Build the union of two or more RDDs. 构建两个或更多RDD的并集*/
  override def union(first: JavaDoubleRDD, rest: java.util.List[JavaDoubleRDD]): JavaDoubleRDD = {
    val rdds: Seq[RDD[Double]] = (Seq(first) ++ asScalaBuffer(rest)).map(_.srdd)
    new JavaDoubleRDD(sc.union(rdds))
  }

  /**
   * Create an [[org.apache.spark.Accumulator]] integer variable, which tasks can "add" values
   * to using the `add` method. Only the master can access the accumulator's `value`.
    * 创建一个[[org.apache.spark.Accumulator]]整数变量,任务可以使用`add`方法“添加”值,只有主设备才能访问累加器的“值”
   */
  def intAccumulator(initialValue: Int): Accumulator[java.lang.Integer] =
    sc.accumulator(initialValue)(IntAccumulatorParam).asInstanceOf[Accumulator[java.lang.Integer]]

  /**
   * Create an [[org.apache.spark.Accumulator]] integer variable, which tasks can "add" values
   * to using the `add` method. Only the master can access the accumulator's `value`.
   *
   * This version supports naming the accumulator for display in Spark's web UI.
   */
  def intAccumulator(initialValue: Int, name: String): Accumulator[java.lang.Integer] =
    sc.accumulator(initialValue, name)(IntAccumulatorParam)
      .asInstanceOf[Accumulator[java.lang.Integer]]

  /**
   * Create an [[org.apache.spark.Accumulator]] double variable, which tasks can "add" values
   * to using the `add` method. Only the master can access the accumulator's `value`.
    * 创建一个[[org.apache.spark.Accumulator]]双变量,任务可以使用`add`方法“添加”值,只有主设备才能访问累加器的“值”
   */
  def doubleAccumulator(initialValue: Double): Accumulator[java.lang.Double] =
    sc.accumulator(initialValue)(DoubleAccumulatorParam).asInstanceOf[Accumulator[java.lang.Double]]

  /**
   * Create an [[org.apache.spark.Accumulator]] double variable, which tasks can "add" values
   * to using the `add` method. Only the master can access the accumulator's `value`.
   *
   * This version supports naming the accumulator for display in Spark's web UI.
   */
  def doubleAccumulator(initialValue: Double, name: String): Accumulator[java.lang.Double] =
    sc.accumulator(initialValue, name)(DoubleAccumulatorParam)
      .asInstanceOf[Accumulator[java.lang.Double]]

  /**
   * Create an [[org.apache.spark.Accumulator]] integer variable, which tasks can "add" values
   * to using the `add` method. Only the master can access the accumulator's `value`.
   */
  def accumulator(initialValue: Int): Accumulator[java.lang.Integer] = intAccumulator(initialValue)

  /**
   * Create an [[org.apache.spark.Accumulator]] integer variable, which tasks can "add" values
   * to using the `add` method. Only the master can access the accumulator's `value`.
   *
   * This version supports naming the accumulator for display in Spark's web UI.
   */
  def accumulator(initialValue: Int, name: String): Accumulator[java.lang.Integer] =
    intAccumulator(initialValue, name)

  /**
   * Create an [[org.apache.spark.Accumulator]] double variable, which tasks can "add" values
   * to using the `add` method. Only the master can access the accumulator's `value`.
   */
  def accumulator(initialValue: Double): Accumulator[java.lang.Double] =
    doubleAccumulator(initialValue)


  /**
   * Create an [[org.apache.spark.Accumulator]] double variable, which tasks can "add" values
   * to using the `add` method. Only the master can access the accumulator's `value`.
   *
   * This version supports naming the accumulator for display in Spark's web UI.
   */
  def accumulator(initialValue: Double, name: String): Accumulator[java.lang.Double] =
    doubleAccumulator(initialValue, name)

  /**
   * Create an [[org.apache.spark.Accumulator]] variable of a given type, which tasks can "add"
   * values to using the `add` method. Only the master can access the accumulator's `value`.
   */
  def accumulator[T](initialValue: T, accumulatorParam: AccumulatorParam[T]): Accumulator[T] =
    sc.accumulator(initialValue)(accumulatorParam)

  /**
   * Create an [[org.apache.spark.Accumulator]] variable of a given type, which tasks can "add"
   * values to using the `add` method. Only the master can access the accumulator's `value`.
   *
   * This version supports naming the accumulator for display in Spark's web UI.
   */
  def accumulator[T](initialValue: T, name: String, accumulatorParam: AccumulatorParam[T])
      : Accumulator[T] =
    sc.accumulator(initialValue, name)(accumulatorParam)

  /**
   * Create an [[org.apache.spark.Accumulable]] shared variable of the given type, to which tasks
   * can "add" values with `add`. Only the master can access the accumuable's `value`.
   */
  def accumulable[T, R](initialValue: T, param: AccumulableParam[T, R]): Accumulable[T, R] =
    sc.accumulable(initialValue)(param)

  /**
   * Create an [[org.apache.spark.Accumulable]] shared variable of the given type, to which tasks
   * can "add" values with `add`. Only the master can access the accumuable's `value`.
   *
   * This version supports naming the accumulator for display in Spark's web UI.
   */
  def accumulable[T, R](initialValue: T, name: String, param: AccumulableParam[T, R])
      : Accumulable[T, R] =
    sc.accumulable(initialValue, name)(param)

  /**
   * Broadcast a read-only variable to the cluster, returning a
   * [[org.apache.spark.broadcast.Broadcast]] object for reading it in distributed functions.
   * The variable will be sent to each cluster only once.
   */
  def broadcast[T](value: T): Broadcast[T] = sc.broadcast(value)(fakeClassTag)

  /** Shut down the SparkContext. 关闭SparkContext*/
  def stop() {
    sc.stop()
  }

  override def close(): Unit = stop()

  /**
   * Get Spark's home location from either a value set through the constructor,
   * or the spark.home Java property, or the SPARK_HOME environment variable
   * (in that order of preference). If neither of these is set, return None.
    *
    * 从通过构造函数设置的值或spark.home Java属性或SPARK_HOME环境变量(按优先顺序)获取Spark的主位置,
    * 如果这两个都未设置,则返回None
   */
  def getSparkHome(): Optional[String] = JavaUtils.optionToOptional(sc.getSparkHome())

  /**
   * Add a file to be downloaded with this Spark job on every node.
    * 在每个节点上添加要使用此Spark作业下载的文件
   * The `path` passed can be either a local file, a file in HDFS (or other Hadoop-supported
   * filesystems), or an HTTP, HTTPS or FTP URI.  To access the file in Spark jobs,
   * use `SparkFiles.get(fileName)` to find its download location.
    * 传递的`path`可以是本地文件,HDFS（或其他Hadoop支持的文件系统）中的文件,
    * 也可以是HTTP，HTTPS或FTP URI,要在Spark作业中访问该文件,请使用`SparkFiles.get（fileName）`来查找其下载位置
   */
  def addFile(path: String) {
    sc.addFile(path)
  }

  /**
   * Adds a JAR dependency for all tasks to be executed on this SparkContext in the future.
    * 为将来要在此SparkContext上执行的所有任务添加JAR依赖项
   * The `path` passed can be either a local file, a file in HDFS (or other Hadoop-supported
   * filesystems), or an HTTP, HTTPS or FTP URI.
    * 传递的`path`可以是本地文件,HDFS(或其他Hadoop支持的文件系统)中的文件,也可以是HTTP,HTTPS或FTP URI
   */
  def addJar(path: String) {
    sc.addJar(path)
  }

  /**
   * Clear the job's list of JARs added by `addJar` so that they do not get downloaded to
   * any new nodes.
    * 清除作业的'addJar`添加的JAR列表,这样它们就不会被下载到清除`addJar`添加的作业的JAR列表,这样它们就不会被下载到
   */
  @deprecated("adding jars no longer creates local copies that need to be deleted", "1.0.0")
  def clearJars() {
    sc.clearJars()
  }

  /**
   * Clear the job's list of files added by `addFile` so that they do not get downloaded to
   * any new nodes.
    * 清除`addFile`添加的作业文件列表,这样它们就不会下载到任何新节点
   */
  @deprecated("adding files no longer creates local copies that need to be deleted", "1.0.0")
  def clearFiles() {
    sc.clearFiles()
  }

  /**
   * Returns the Hadoop configuration used for the Hadoop code (e.g. file systems) we reuse.
    * 返回用于我们重用的Hadoop代码(例如文件系统)的Hadoop配置
   *
   * '''Note:''' As it will be reused in all Hadoop RDDs, it's better not to modify it unless you
   * plan to set some global configurations for all Hadoop RDDs.
   */
  def hadoopConfiguration(): Configuration = {
    sc.hadoopConfiguration
  }

  /**
   * Set the directory under which RDDs are going to be checkpointed. The directory must
   * be a HDFS path if running on a cluster.
    * 设置要检查点RDD的目录,如果在群集上运行,则该目录必须是HDFS路径
   */
  def setCheckpointDir(dir: String) {
    sc.setCheckpointDir(dir)
  }

  def getCheckpointDir: Optional[String] = JavaUtils.optionToOptional(sc.getCheckpointDir)

  protected def checkpointFile[T](path: String): JavaRDD[T] = {
    implicit val ctag: ClassTag[T] = fakeClassTag
    new JavaRDD(sc.checkpointFile(path))
  }

  /**
   * Return a copy of this JavaSparkContext's configuration. The configuration ''cannot'' be
   * changed at runtime.
    * 返回此JavaSparkContext配置的副本,配置''不能'在运行时更改
   */
  def getConf: SparkConf = sc.getConf

  /**
   * Pass-through to SparkContext.setCallSite.  For API support only.
    * 传递给SparkContext.setCallSite,仅限API支持
   */
  def setCallSite(site: String) {
    sc.setCallSite(site)
  }

  /**
   * Pass-through to SparkContext.setCallSite.  For API support only.
    * 传递给SparkContext.setCallSite。 仅限API支持
   */
  def clearCallSite() {
    sc.clearCallSite()
  }

  /**
   * Set a local property that affects jobs submitted from this thread, such as the
   * Spark fair scheduler pool.
    * 设置影响从此线程提交的作业的本地属性,例如Spark fair调度程序池
   */
  def setLocalProperty(key: String, value: String): Unit = sc.setLocalProperty(key, value)

  /**
   * Get a local property set in this thread, or null if it is missing. See
    * 获取此线程中的本地属性集,如果缺少则为null
   * [[org.apache.spark.api.java.JavaSparkContext.setLocalProperty]].
   */
  def getLocalProperty(key: String): String = sc.getLocalProperty(key)

  /** Control our logLevel. This overrides any user-defined log settings.
    * 控制我们的logLevel,这将覆盖任何用户定义的日志设置
   * @param logLevel The desired log level as a string.
   * Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
   */
  def setLogLevel(logLevel: String) {
    sc.setLogLevel(logLevel)
  }

  /**
   * Assigns a group ID to all the jobs started by this thread until the group ID is set to a
   * different value or cleared.
    * 为此线程启动的所有作业分配组ID,直到组ID设置为其他值或清除为止
   *
   * Often, a unit of execution in an application consists of multiple Spark actions or jobs.
   * Application programmers can use this method to group all those jobs together and give a
   * group description. Once set, the Spark web UI will associate such jobs with this group.
   *
   * The application can also use [[org.apache.spark.api.java.JavaSparkContext.cancelJobGroup]]
   * to cancel all running jobs in this group. For example,
   * {{{
   * // In the main thread:
   * sc.setJobGroup("some_job_to_cancel", "some job description");
   * rdd.map(...).count();
   *
   * // In a separate thread:
   * sc.cancelJobGroup("some_job_to_cancel");
   * }}}
   *
   * If interruptOnCancel is set to true for the job group, then job cancellation will result
   * in Thread.interrupt() being called on the job's executor threads. This is useful to help ensure
   * that the tasks are actually stopped in a timely manner, but is off by default due to HDFS-1208,
   * where HDFS may respond to Thread.interrupt() by marking nodes as dead.
   */
  def setJobGroup(groupId: String, description: String, interruptOnCancel: Boolean): Unit =
    sc.setJobGroup(groupId, description, interruptOnCancel)

  /**
   * Assigns a group ID to all the jobs started by this thread until the group ID is set to a
   * different value or cleared.
    * 为此线程启动的所有作业分配组ID,直到组ID设置为其他值或清除为止
   *
   * @see `setJobGroup(groupId: String, description: String, interruptThread: Boolean)`.
   *      This method sets interruptOnCancel to false.
   */
  def setJobGroup(groupId: String, description: String): Unit = sc.setJobGroup(groupId, description)

  /** Clear the current thread's job group ID and its description.
    * 清除当前线程的作业组ID及其描述*/
  def clearJobGroup(): Unit = sc.clearJobGroup()

  /**
   * Cancel active jobs for the specified group. See
   * [[org.apache.spark.api.java.JavaSparkContext.setJobGroup]] for more information.
    * 取消指定组的活动作业
   */
  def cancelJobGroup(groupId: String): Unit = sc.cancelJobGroup(groupId)

  /** Cancel all jobs that have been scheduled or are running.
    * 取消已安排或正在运行的所有作业 */
  def cancelAllJobs(): Unit = sc.cancelAllJobs()
}

object JavaSparkContext {
  implicit def fromSparkContext(sc: SparkContext): JavaSparkContext = new JavaSparkContext(sc)

  implicit def toSparkContext(jsc: JavaSparkContext): SparkContext = jsc.sc

  /**
   * Find the JAR from which a given class was loaded, to make it easy for users to pass
   * their JARs to SparkContext.
    * 找到加载了给定类的JAR,以便用户可以轻松地将其JAR传递给SparkContext
   */
  def jarOfClass(cls: Class[_]): Array[String] = SparkContext.jarOfClass(cls).toArray

  /**
   * Find the JAR that contains the class of a particular object, to make it easy for users
   * to pass their JARs to SparkContext. In most cases you can call jarOfObject(this) in
   * your driver program.
    *
    * 找到包含特定对象类的JAR,以便用户可以轻松地将其JAR传递给SparkContext,在大多数情况下,您可以在驱动程序中调用jarOfObject(this)
   */
  def jarOfObject(obj: AnyRef): Array[String] = SparkContext.jarOfObject(obj).toArray

  /**
   * Produces a ClassTag[T], which is actually just a casted ClassTag[AnyRef].
    * 产生一个ClassTag [T],它实际上只是一个铸造的ClassTag [AnyRef]
   *
   * This method is used to keep ClassTags out of the external Java API, as the Java compiler
   * cannot produce them automatically. While this ClassTag-faking does please the compiler,
   * it can cause problems at runtime if the Scala API relies on ClassTags for correctness.
    *
    * 此方法用于将ClassTag保留在外部Java API之外,因为Java编译器无法自动生成它们,
    * 虽然这种ClassTag-faking确实让编译器满意,但如果Scala API依赖于ClassTag的正确性,它可能会在运行时引发问题
   *
   * Often, though, a ClassTag[AnyRef] will not lead to incorrect behavior, just worse performance
   * or security issues. For instance, an Array[AnyRef] can hold any type T, but may lose primitive
   * specialization.
   */
  private[spark]
  def fakeClassTag[T]: ClassTag[T] = ClassTag.AnyRef.asInstanceOf[ClassTag[T]]
}
