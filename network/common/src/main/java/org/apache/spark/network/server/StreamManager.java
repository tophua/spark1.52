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

package org.apache.spark.network.server;

import io.netty.channel.Channel;

import org.apache.spark.network.buffer.ManagedBuffer;

/**
 * The StreamManager is used to fetch individual chunks from a stream. This is used in
 * {@link TransportRequestHandler} in order to respond to fetchChunk() requests. Creation of the
 * stream is outside the scope of the transport layer, but a given stream is guaranteed to be read
 * by only one client connection, meaning that getChunk() for a particular stream will be called
 * serially and that once the connection associated with the stream is closed, that stream will
 * never be used again.
 *
 * StreamManager用于从流中获取单个块,这是用在{@link TransportRequestHandler}以响应fetchChunk（）请求。
 * 创造流不在传输层的范围之内,但保证给定的流被读取,只有一个客户端连接，这意味着一个特定流的getChunk（）将被调用
 * 连续地,一旦与流关联的连接关闭,该流将会再也不用了
 */
public abstract class StreamManager {
  /**
   * Called in response to a fetchChunk() request. The returned buffer will be passed as-is to the
   * client. A single stream will be associated with a single TCP connection, so this method
   * will not be called in parallel for a particular stream.
   *
   * 调用响应fetchChunk()请求,返回的缓冲区将按原样传递给客户端,
   * 单个流将与单个TCP连接相关联,因此对于特定流不会并行调用此方法,
   *
   * Chunks may be requested in any order, and requests may be repeated, but it is not required
   * that implementations support this behavior.
   * 可以按任何顺序请求块,并且可能会重复请求,但不要求实现支持此行为,
   *
   * The returned ManagedBuffer will be release()'d after being written to the network.
   * 返回的ManagedBuffer在写入网络后将被释放()
   *
   * @param streamId id of a stream that has been previously registered with the StreamManager.
   * @param chunkIndex 0-indexed chunk of the stream that's requested
   */
  public abstract ManagedBuffer getChunk(long streamId, int chunkIndex);

  /**
   * Associates a stream with a single client connection, which is guaranteed to be the only reader
   * of the stream. The getChunk() method will be called serially on this connection and once the
   * connection is closed, the stream will never be used again, enabling cleanup.
   *
   * 将流与单个客户端连接关联,这被保证是流的唯一读者,getChunk()方法将在此连接上串行调用,
   * 一旦连接关闭，流将永远不会被再次使用，从而实现清理。
   *
   * This must be called before the first getChunk() on the stream, but it may be invoked multiple
   * times with the same channel and stream id.
   * 这必须在流上的第一个getChunk()之前调用,但是可以使用相同的通道和流ID来调用多次,
   */
  public void registerChannel(Channel channel, long streamId) { }

  /**
   * Indicates that the given channel has been terminated. After this occurs, we are guaranteed not
   * to read from the associated streams again, so any state can be cleaned up.
   * 表示给定的频道已被终止,发生这种情况后,我们保证不会再次从关联的流中读取,所以任何状态都可以被清理。
   */
  public void connectionTerminated(Channel channel) { }
}
