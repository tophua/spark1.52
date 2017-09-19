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

package org.apache.spark.network.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * This interface provides an immutable view for data in the form of bytes. The implementation
 * should specify how the data is provided:
 * 此接口提供了以字节形式的数据的不可变视图,实现应指定如何提供数据：
 *
 * - {@link FileSegmentManagedBuffer}: data backed by part of a file,由文件的一部分支持的数据
 * - {@link NioManagedBuffer}: data backed by a NIO ByteBuffer 数据由NIO ByteBuffer支持
 * - {@link NettyManagedBuffer}: data backed by a Netty ByteBuf Netty ByteBuf支持的数据
 *
 * The concrete buffer implementation might be managed outside the JVM garbage collector.
 * For example, in the case of {@link NettyManagedBuffer}, the buffers are reference counted.
 * In that case, if the buffer is going to be passed around to a different thread, retain/release
 * should be called.
 * 具体的缓冲区实现可能在JVM垃圾收集器外部进行管理,例如，在{@link NettyManagedBuffer}的情况下,缓冲区被引用计数,
 * 在这种情况下,如果要将缓冲区传递给不同的线程,则应该调用保留/释放。
 *
 * 对于Network通信,不管传输的是序列化后的对象还是文件,在网络上表现的都是字节流。
 * 在传统IO中,字节流表示为Stream;在NIO中,字节流表示为ByteBuffer;
 * 在Netty中字节流表示为ByteBuff或FileRegion;在Spark中,针对Byte也做了一层包装,支持对Byte和文件流进行处理,即ManagedBuffer
 */
public abstract class ManagedBuffer {

  /** Number of bytes of the data.
   * 数据的字节数*/
  public abstract long size();

  /**
   * Exposes this buffer's data as an NIO ByteBuffer. Changing the position and limit of the
   * returned ByteBuffer should not affect the content of this buffer.
   * 将这个缓冲区的数据暴露为NIO ByteBuffer
   * 改变位置和限制返回ByteBuffer不应该影响这个缓冲区的内容
   */
  // TODO: Deprecate this, usage may require expensive memory mapping or allocation.
  public abstract ByteBuffer nioByteBuffer() throws IOException;

  /**
   * Exposes this buffer's data as an InputStream. The underlying implementation does not
   * necessarily check for the length of bytes read, so the caller is responsible for making sure
   * it does not go over the limit.
   * 将此缓冲区的数据显示为InputStream,底层实现不一定要检查读取的字节长度,所以调用者负责确保它不超过限制。
   * createInputStream来对Buffer进行“类型转换”stream
   */
  public abstract InputStream createInputStream() throws IOException;

  /**
   * Increment the reference count by one if applicable.
   * 如果适用,将引用计数增加1
   */
  public abstract ManagedBuffer retain();

  /**
   * If applicable, decrement the reference count by one and deallocates the buffer if the
   * reference count reaches zero.
   * 如果适用,将引用计数递减1,如果引用计数达到零,则释放缓冲区。
   */
  public abstract ManagedBuffer release();

  /**
   * Convert the buffer into an Netty object, used to write the data out.
   * 将缓冲区转换为Netty对象,用于写入数据
   */
  public abstract Object convertToNetty() throws IOException;
}
