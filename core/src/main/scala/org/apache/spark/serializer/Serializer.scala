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

package org.apache.spark.serializer

import java.io._
import java.nio.ByteBuffer
import javax.annotation.concurrent.NotThreadSafe

import scala.reflect.ClassTag

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.annotation.{DeveloperApi, Private}
import org.apache.spark.util.{Utils, ByteBufferInputStream, NextIterator}

/**
 * :: DeveloperApi ::
 * A serializer. Because some serialization libraries are not thread safe, this class is used to
 * create [[org.apache.spark.serializer.SerializerInstance]] objects that do the actual
 * serialization and are guaranteed to only be called from one thread at a time.
 * 一个序列化, 因为一些序列化库不是线程安全的,所以这个类用于创建实际实现的[[org.apache.spark.serializer.SerializerInstance]]
  * 对象序列化,并保证只能从一个线程一次调用。
 * Implementations of this trait should implement:
 *  这种特征的实施应该实现：
 * 1. a zero-arg constructor or a constructor that accepts a [[org.apache.spark.SparkConf]]
 * as parameter. If both constructors are defined, the latter takes precedence.
 *    零对象构造函数或构造函数接受[[org.apache.spark.SparkConf]]作为参数,如果两个构造函数都被定义，则后者优先
  *
 * 2. Java serialization interface. Java序列化接口
 *
 * Note that serializers are not required to be wire-compatible across different versions of Spark.
 * They are intended to be used to serialize/de-serialize data within a single Spark application.
  * 请注意,序列化程序不需要在不同版本的Spark之间进行线路兼容,它们旨在用于在单个Spark应用程序中对数据进行序列化/解串行化。
 */
@DeveloperApi
abstract class Serializer {

  /**
   * Default ClassLoader to use in deserialization. Implementations of [[Serializer]] should
   * make sure it is using this when set.
    * 用于反序列化的默认ClassLoader,[[Serializer]]的实现应该确保在设置时使用它
   */
  @volatile protected var defaultClassLoader: Option[ClassLoader] = None

  /**
   * Sets a class loader for the serializer to use in deserialization.
    * 设置用于反序列化的序列化程序的类加载器
   *
   * @return this Serializer object
   */
  def setDefaultClassLoader(classLoader: ClassLoader): Serializer = {
    defaultClassLoader = Some(classLoader)
    this
  }

  /** Creates a new [[SerializerInstance]]. */
  def newInstance(): SerializerInstance

  /**
   * :: Private ::
   * Returns true if this serializer supports relocation of its serialized objects and false
   * otherwise. This should return true if and only if reordering the bytes of serialized objects
   * in serialization stream output is equivalent to having re-ordered those elements prior to
   * serializing them. More specifically, the following should hold if a serializer supports
   * relocation:
   * 如果此序列化器支持其序列化对象的重定位，则返回true，否则返回false。 当且仅当重新排序序列化对象的字节时，这应该返回true
    *在序列化流输出中相当于在之前重新排序了这些元素
    *将它们序列化 更具体地说，如果序列化程序支持重定位，则应该保留以下内容：
   * {{{
   * serOut.open()
   * position = 0
   * serOut.write(obj1)
   * serOut.flush()
   * position = # of bytes writen to stream so far
   * obj1Bytes = output[0:position-1]
   * serOut.write(obj2)
   * serOut.flush()
   * position2 = # of bytes written to stream so far
   * obj2Bytes = output[position:position2-1]
   * serIn.open([obj2bytes] concatenate [obj1bytes]) should return (obj2, obj1)
   * }}}
   *
   * In general, this property should hold for serializers that are stateless and that do not
   * write special metadata at the beginning or end of the serialization stream.
    * 一般来说,这个属性应该适用于无状态的序列化程序在序列化流的开头或结尾写入特殊元数据。
   *
   * This API is private to Spark; this method should not be overridden in third-party subclasses
   * or called in user code and is subject to removal in future Spark releases.
    * 此API对于Spark是私有的, 此方法不应在第三方子类中被覆盖或者在用户代码中调用,并且在将来的Spark版本中被删除。
   *
   * See SPARK-7311 for more details.
   */
  @Private
  private[spark] def supportsRelocationOfSerializedObjects: Boolean = false
}


@DeveloperApi
object Serializer {
  def getSerializer(serializer: Serializer): Serializer = {
    if (serializer == null) SparkEnv.get.serializer else serializer
  }

  def getSerializer(serializer: Option[Serializer]): Serializer = {
    serializer.getOrElse(SparkEnv.get.serializer)
  }
}


/**
 * :: DeveloperApi ::
 * An instance of a serializer, for use by one thread at a time.
  * 一个串行器的实例,一次由一个线程使用
 *
 * It is legal to create multiple serialization / deserialization streams from the same
 * SerializerInstance as long as those streams are all used within the same thread.
  * 只要这些流都在同一个线程中使用,就可以从同一个SerializerInstance创建多个序列化/反序列化流。
 */
@DeveloperApi
@NotThreadSafe
abstract class SerializerInstance {
  def serialize[T: ClassTag](t: T): ByteBuffer

  def deserialize[T: ClassTag](bytes: ByteBuffer): T

  def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T

  def serializeStream(s: OutputStream): SerializationStream

  def deserializeStream(s: InputStream): DeserializationStream
}

/**
 * :: DeveloperApi ::
 * A stream for writing serialized objects.
  * 用于编写序列化对象的流
 */
@DeveloperApi
abstract class SerializationStream {
  /** The most general-purpose method to write an object.
    * 最通用的方法来写一个对象 */
  def writeObject[T: ClassTag](t: T): SerializationStream
  /** Writes the object representing the key of a key-value pair.
    * 写入表示键值对键的对象 */
  def writeKey[T: ClassTag](key: T): SerializationStream = writeObject(key)
  /** Writes the object representing the value of a key-value pair.
    * 写入表示键值对的值的对象 */
  def writeValue[T: ClassTag](value: T): SerializationStream = writeObject(value)
  def flush(): Unit
  def close(): Unit

  def writeAll[T: ClassTag](iter: Iterator[T]): SerializationStream = {
    while (iter.hasNext) {
      writeObject(iter.next())
    }
    this
  }
}


/**
 * :: DeveloperApi ::
 * A stream for reading serialized objects.
  * 用于读取序列化对象的流
 */
@DeveloperApi
abstract class DeserializationStream {
  /** The most general-purpose method to read an object.
    * 读取对象的最通用的方法 */
  def readObject[T: ClassTag](): T
  /** Reads the object representing the key of a key-value pair.
    * 读取表示键值对的键的对象 */
  def readKey[T: ClassTag](): T = readObject[T]()
  /** Reads the object representing the value of a key-value pair.
    * 读取表示键值对的值的对象*/
  def readValue[T: ClassTag](): T = readObject[T]()
  def close(): Unit

  /**
   * Read the elements of this stream through an iterator. This can only be called once, as
   * reading each element will consume data from the input source.
    * 通过迭代器读取此流的元素,这只能调用一次,因为读取每个元素将从输入源消耗数据。
   */
  def asIterator: Iterator[Any] = new NextIterator[Any] {
    override protected def getNext() = {
      try {
        readObject[Any]()
      } catch {
        case eof: EOFException =>
          finished = true
          null
      }
    }

    override protected def close() {
      DeserializationStream.this.close()
    }
  }

  /**
   * Read the elements of this stream through an iterator over key-value pairs. This can only be
   * called once, as reading each element will consume data from the input source.
    * 通过键值对上的迭代器读取此流的元素,这只能调用一次,因为读取每个元素将从输入源消耗数据。
   */
  def asKeyValueIterator: Iterator[(Any, Any)] = new NextIterator[(Any, Any)] {
    override protected def getNext() = {
      try {
        (readKey[Any](), readValue[Any]())
      } catch {
        case eof: EOFException => {
          finished = true
          null
        }
      }
    }

    override protected def close() {
      DeserializationStream.this.close()
    }
  }
}
