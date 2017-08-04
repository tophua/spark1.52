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

package org.apache.spark.network.client;

import org.apache.spark.network.buffer.ManagedBuffer;

/**
 * Callback for the result of a single chunk result. For a single stream, the callbacks are
 * guaranteed to be called by the same thread in the same order as the requests for chunks were
 * made.
 * 回调为单个块结果的结果,对于单个流,回调是保证被同一个线程按照与块的请求相同的顺序被调用。
 *
 * Note that if a general stream failure occurs, all outstanding chunk requests may be failed.
 * 请注意,如果发生常规流故障,所有未完成的块请求可能失败
 */
public interface ChunkReceivedCallback {
  /**
   * Called upon receipt of a particular chunk.
   * 在收到特定的块后调用
   *
   * The given buffer will initially have a refcount of 1, but will be release()'d as soon as this
   * call returns. You must therefore either retain() the buffer or copy its contents before
   * returning.
   * 给定的缓冲区最初将具有1的计数,但是一旦该调用返回,它将被释放（）'d。 因此,您必须在返回之前保留（）缓冲区或复制其内容
   */
  void onSuccess(int chunkIndex, ManagedBuffer buffer);

  /**
   * Called upon failure to fetch a particular chunk. Note that this may actually be called due
   * to failure to fetch a prior chunk in this stream.
   * 在未能获取特定块时调用,请注意,由于无法获取此流中的先前块,实际上可能会调用此方法
   * After receiving a failure, the stream may or may not be valid. The client should not assume
   * that the server's side of the stream has been closed.
   * 收到故障后,流可能有效也可能无效,客户端不应该假定流的服务器端已经关闭
   */
  void onFailure(int chunkIndex, Throwable e);
}
