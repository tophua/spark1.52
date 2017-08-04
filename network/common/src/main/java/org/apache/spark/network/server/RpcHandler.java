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

import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;

/**
 * Handler for sendRPC() messages sent by {@link org.apache.spark.network.client.TransportClient}s.
 * 负责shuffle的I/O服务端在接收客户端的RPC请求后,提供下载Block或者上传Block的RPC处理,此处即NettyBlockRPCServer
 */
public abstract class RpcHandler {
  /**
   * Receive a single RPC message. Any exception thrown while in this method will be sent back to
   * the client in string form as a standard RPC failure.
   * 接收RPC消息,同时该方法抛出的任何异常将被发送回客户端的字符串形式作为一个标准的RPC失败
   * This method will not be called in parallel for a single TransportClient (i.e., channel).
   * 对于单个TransportClient(即，通道),不会并行调用此方法
   *
   * @param client A channel client which enables the handler to make requests back to the sender
   *               of this RPC. This will always be the exact same object for a particular channel.
   * @param message The serialized bytes of the RPC.
   * @param callback Callback which should be invoked exactly once upon success or failure of the
   *                 RPC.
   */
  public abstract void receive(
      TransportClient client,
      byte[] message,
      RpcResponseCallback callback);

  /**
   * Returns the StreamManager which contains the state about which streams are currently being
   * fetched by a TransportClient.
   * 返回StreamManager,它包含当前正由TransportClient获取的流的状态
   */
  public abstract StreamManager getStreamManager();

  /**
   * Invoked when the connection associated with the given client has been invalidated.
   * No further requests will come from this client.
   * 当与给定客户端关联的连接已被无效时调用,不再需要此客户端的请求
   */
  public void connectionTerminated(TransportClient client) { }
}
