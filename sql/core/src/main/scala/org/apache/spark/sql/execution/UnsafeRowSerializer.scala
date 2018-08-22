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

package org.apache.spark.sql.execution

import java.io._
import java.nio.ByteBuffer

import scala.reflect.ClassTag

import com.google.common.io.ByteStreams

import org.apache.spark.serializer.{SerializationStream, DeserializationStream, SerializerInstance, Serializer}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.unsafe.Platform

/**
 * Serializer for serializing [[UnsafeRow]]s during shuffle. Since UnsafeRows are already stored as
 * bytes, this serializer simply copies those bytes to the underlying output stream. When
 * deserializing a stream of rows, instances of this serializer mutate and return a single UnsafeRow
 * instance that is backed by an on-heap byte array.
  *
  * 用于在随机播放期间序列化[[UnsafeRow]]的序列化程序,由于UnsafeRows已经存储为字节,
  * 因此该序列化程序只是将这些字节复制到基础输出流,反序列化行流时,此序列化程序的实例会发生变异,
  * 并返回由堆内字节数组支持的单个UnsafeRow实例。
 *
 * Note that this serializer implements only the [[Serializer]] methods that are used during
 * shuffle, so certain [[SerializerInstance]] methods will throw UnsupportedOperationException.
 *
 * @param numFields the number of fields in the row being serialized.
 */
private[sql] class UnsafeRowSerializer(numFields: Int) extends Serializer with Serializable {
  override def newInstance(): SerializerInstance = new UnsafeRowSerializerInstance(numFields)
  override private[spark] def supportsRelocationOfSerializedObjects: Boolean = true
}

private class UnsafeRowSerializerInstance(numFields: Int) extends SerializerInstance {
  /**
   * Serializes a stream of UnsafeRows. Within the stream, each record consists of a record
   * length (stored as a 4-byte integer, written high byte first), followed by the record's bytes.
    * 序列化UnsafeRows流,在流中,每个记录包含一个记录长度(存储为4字节整数,先写入高字节)然后是记录的字节。
   */
  override def serializeStream(out: OutputStream): SerializationStream = new SerializationStream {
    private[this] var writeBuffer: Array[Byte] = new Array[Byte](4096)
    private[this] val dOut: DataOutputStream =
      new DataOutputStream(new BufferedOutputStream(out))

    override def writeValue[T: ClassTag](value: T): SerializationStream = {
      val row = value.asInstanceOf[UnsafeRow]

      dOut.writeInt(row.getSizeInBytes)
      row.writeToStream(dOut, writeBuffer)
      this
    }

    override def writeKey[T: ClassTag](key: T): SerializationStream = {
      // The key is only needed on the map side when computing partition ids. It does not need to
      // be shuffled.
      //只有在计算分区ID时才需要在映射端使用密钥,它不需要洗牌
      assert(null == key || key.isInstanceOf[Int])
      this
    }

    override def writeAll[T: ClassTag](iter: Iterator[T]): SerializationStream = {
      // This method is never called by shuffle code.
      //shuffle代码从不调用此方法
      throw new UnsupportedOperationException
    }

    override def writeObject[T: ClassTag](t: T): SerializationStream = {
      // This method is never called by shuffle code.
      //shuffle代码从不调用此方法
      throw new UnsupportedOperationException
    }

    override def flush(): Unit = {
      dOut.flush()
    }

    override def close(): Unit = {
      writeBuffer = null
      dOut.close()
    }
  }

  override def deserializeStream(in: InputStream): DeserializationStream = {
    new DeserializationStream {
      private[this] val dIn: DataInputStream = new DataInputStream(new BufferedInputStream(in))
      // 1024 is a default buffer size; this buffer will grow to accommodate larger rows
      //1024是默认缓冲区大小; 此缓冲区将增长以容纳更大的行
      private[this] var rowBuffer: Array[Byte] = new Array[Byte](1024)
      private[this] var row: UnsafeRow = new UnsafeRow()
      private[this] var rowTuple: (Int, UnsafeRow) = (0, row)
      private[this] val EOF: Int = -1

      override def asKeyValueIterator: Iterator[(Int, UnsafeRow)] = {
        new Iterator[(Int, UnsafeRow)] {

          private[this] def readSize(): Int = try {
            dIn.readInt()
          } catch {
            case e: EOFException =>
              dIn.close()
              EOF
          }

          private[this] var rowSize: Int = readSize()
          override def hasNext: Boolean = rowSize != EOF

          override def next(): (Int, UnsafeRow) = {
            if (rowBuffer.length < rowSize) {
              rowBuffer = new Array[Byte](rowSize)
            }
            ByteStreams.readFully(dIn, rowBuffer, 0, rowSize)
            row.pointTo(rowBuffer, Platform.BYTE_ARRAY_OFFSET, numFields, rowSize)
            rowSize = readSize()
            if (rowSize == EOF) { // We are returning the last row in this stream
              dIn.close()
              val _rowTuple = rowTuple
              // Null these out so that the byte array can be garbage collected once the entire
              // iterator has been consumed
              //将它们取出以使得一旦消耗掉整个迭代器,就可以对字节数组进行垃圾收集
              row = null
              rowBuffer = null
              rowTuple = null
              _rowTuple
            } else {
              rowTuple
            }
          }
        }
      }

      override def asIterator: Iterator[Any] = {
        // This method is never called by shuffle code.
        //shuffle代码从不调用此方法
        throw new UnsupportedOperationException
      }

      override def readKey[T: ClassTag](): T = {
        // We skipped serialization of the key in writeKey(), so just return a dummy value since
        // this is going to be discarded anyways.
        //我们在writeKey()中跳过了键的序列化,所以只返回一个虚拟值,因为它会被丢弃
        null.asInstanceOf[T]
      }

      override def readValue[T: ClassTag](): T = {
        val rowSize = dIn.readInt()
        if (rowBuffer.length < rowSize) {
          rowBuffer = new Array[Byte](rowSize)
        }
        ByteStreams.readFully(dIn, rowBuffer, 0, rowSize)
        row.pointTo(rowBuffer, Platform.BYTE_ARRAY_OFFSET, numFields, rowSize)
        row.asInstanceOf[T]
      }

      override def readObject[T: ClassTag](): T = {
        // This method is never called by shuffle code.
        //shuffle代码从不调用此方法
        throw new UnsupportedOperationException
      }

      override def close(): Unit = {
        dIn.close()
      }
    }
  }

  // These methods are never called by shuffle code.
  //shuffle代码从不调用这些方法
  override def serialize[T: ClassTag](t: T): ByteBuffer = throw new UnsupportedOperationException
  override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
    throw new UnsupportedOperationException
  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
    throw new UnsupportedOperationException
}
