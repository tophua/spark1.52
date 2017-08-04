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

import java.util.EventListener;

import org.apache.spark.network.buffer.ManagedBuffer;

public interface BlockFetchingListener extends EventListener {
  /**
   * Called once per successfully fetched block. After this call returns, data will be released
   * automatically. If the data will be passed to another thread, the receiver should retain()
   * and release() the buffer on their own, or copy the data to a new buffer.
   * 被成功抓取一次,此通话返回后,数据将自动释放,如果数据将被传递给另一个线程，接收方应该保留retain()和释放release()缓冲区
   * 或将数据复制到新的缓冲区。
   */
  void onBlockFetchSuccess(String blockId, ManagedBuffer data);

  /**
   * Called at least once per block upon failures.
   * 失败后至少每隔一次调用一次
   */
  void onBlockFetchFailure(String blockId, Throwable exception);
}
