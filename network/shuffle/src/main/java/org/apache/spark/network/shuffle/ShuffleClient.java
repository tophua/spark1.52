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

package org.apache.spark.network.shuffle;

import java.io.Closeable;

/** Provides an interface for reading shuffle files, either from an Executor or external service.
 * 提供一个从Executor或外部服务读取随机播放文件的界面*/
public abstract class ShuffleClient implements Closeable {

  /**
   * Initializes the ShuffleClient, specifying this Executor's appId.
   * Must be called before any other method on the ShuffleClient.
   * 初始化ShuffleClient,指定此Executor的appId,必须在ShuffleClient上的任何其他方法之前调用
   */
  public void init(String appId) { }

  /**
   * Fetch a sequence of blocks from a remote node asynchronously,
   * 从远程节点异步获取块序列
   *
   * Note that this API takes a sequence so the implementation can batch requests, and does not
   * return a future so the underlying implementation can invoke onBlockFetchSuccess as soon as
   * the data of a block is fetched, rather than waiting for all blocks to be fetched.
   *
   * 请注意,该API采用一个序列,因此实现可以批处理请求,并且不返回未来,因此一旦获取块的数据,
   * 底层实现就可以调用onBlockFetchSuccess,而不是等待所有块的获取。
   */
  public abstract void fetchBlocks(
      String host,
      int port,
      String execId,
      String[] blockIds,
      BlockFetchingListener listener);
}
