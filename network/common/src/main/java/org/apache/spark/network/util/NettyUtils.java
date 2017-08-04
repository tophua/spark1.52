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

package org.apache.spark.network.util;

import java.lang.reflect.Field;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.internal.PlatformDependent;

/**
 * Utilities for creating various Netty constructs based on whether we're using EPOLL or NIO.
 * 基于我们是否使用EPOLL或NIO来创建各种Netty结构的实用工具
 */
public class NettyUtils {
  /** Creates a new ThreadFactory which prefixes each thread with the given name.
   * 创建一个新的ThreadFactory，它为每个具有给定名称的线程提供前缀*/
  public static ThreadFactory createThreadFactory(String threadPoolPrefix) {
    return new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat(threadPoolPrefix + "-%d")
      .build();
  }

  /** Creates a Netty EventLoopGroup based on the IOMode.
   * 基于IOMode创建一个Netty EventLoopGroup*/
  public static EventLoopGroup createEventLoop(IOMode mode, int numThreads, String threadPrefix) {
    ThreadFactory threadFactory = createThreadFactory(threadPrefix);

    switch (mode) {
      case NIO:
        return new NioEventLoopGroup(numThreads, threadFactory);
      case EPOLL:
        return new EpollEventLoopGroup(numThreads, threadFactory);
      default:
        throw new IllegalArgumentException("Unknown io mode: " + mode);
    }
  }

  /** Returns the correct (client) SocketChannel class based on IOMode.
   * 返回基于IOMode的正确（客户端）SocketChannel类 */
  public static Class<? extends Channel> getClientChannelClass(IOMode mode) {
    switch (mode) {
      case NIO:
        return NioSocketChannel.class;
      case EPOLL:
        return EpollSocketChannel.class;
      default:
        throw new IllegalArgumentException("Unknown io mode: " + mode);
    }
  }

  /** Returns the correct ServerSocketChannel class based on IOMode.
   * 返回基于IOMode的正确的ServerSocketChannel类 */
  public static Class<? extends ServerChannel> getServerChannelClass(IOMode mode) {
    switch (mode) {
      case NIO:
        return NioServerSocketChannel.class;
      case EPOLL:
        return EpollServerSocketChannel.class;
      default:
        throw new IllegalArgumentException("Unknown io mode: " + mode);
    }
  }

  /**
   * Creates a LengthFieldBasedFrameDecoder where the first 8 bytes are the length of the frame.
   * This is used before all decoders.
   * 创建一个LengthFieldBasedFrameDecoder,其中前8个字节是帧的长度,这是在所有解码器之前使用的
   */
  public static ByteToMessageDecoder createFrameDecoder() {
    // maxFrameLength = 2G
    // lengthFieldOffset = 0
    // lengthFieldLength = 8
    // lengthAdjustment = -8, i.e. exclude the 8 byte length itself
    // initialBytesToStrip = 8, i.e. strip out the length field itself
    return new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 8, -8, 8);
  }

  /** Returns the remote address on the channel or "&lt;unknown remote&gt;" if none exists.
   * 返回通道上的远程地址或“＆lt; unknown remote＆gt;” 如果不存 */
  public static String getRemoteAddress(Channel channel) {
    if (channel != null && channel.remoteAddress() != null) {
      return channel.remoteAddress().toString();
    }
    return "<unknown remote>";
  }

  /**
   * Create a pooled ByteBuf allocator but disables the thread-local cache. Thread-local caches
   * are disabled for TransportClients because the ByteBufs are allocated by the event loop thread,
   * but released by the executor thread rather than the event loop thread. Those thread-local
   * caches actually delay the recycling of buffers, leading to larger memory usage.
   *
   * 创建一个池的ByteBuf分配器,但禁用线程本地缓存, 由于ByteBufs由事件循环线程分配,
   * 而是由执行程序线程而不是事件循环线程释放,因此传输客户端的线程本地缓存被禁用,
   * 这些线程本地缓存实际上延迟了缓冲区的回收,导致更大的内存使用。
   */
  public static PooledByteBufAllocator createPooledByteBufAllocator(
      boolean allowDirectBufs,
      boolean allowCache,
      int numCores) {
    if (numCores == 0) {
        //Runtime.getRuntime().availableProcessors()方法获得当前设备的CPU个数
      numCores = Runtime.getRuntime().availableProcessors();
    }
    return new PooledByteBufAllocator(
      allowDirectBufs && PlatformDependent.directBufferPreferred(),
      Math.min(getPrivateStaticField("DEFAULT_NUM_HEAP_ARENA"), numCores),
      Math.min(getPrivateStaticField("DEFAULT_NUM_DIRECT_ARENA"), allowDirectBufs ? numCores : 0),
      getPrivateStaticField("DEFAULT_PAGE_SIZE"),
      getPrivateStaticField("DEFAULT_MAX_ORDER"),
      allowCache ? getPrivateStaticField("DEFAULT_TINY_CACHE_SIZE") : 0,
      allowCache ? getPrivateStaticField("DEFAULT_SMALL_CACHE_SIZE") : 0,
      allowCache ? getPrivateStaticField("DEFAULT_NORMAL_CACHE_SIZE") : 0
    );
  }

  /** Used to get defaults from Netty's private static fields.
   * 用于从Netty的私有静态字段获取默认值*/
  private static int getPrivateStaticField(String name) {
    try {
      Field f = PooledByteBufAllocator.DEFAULT.getClass().getDeclaredField(name);
      f.setAccessible(true);
      return f.getInt(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
