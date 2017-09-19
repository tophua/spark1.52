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

import org.apache.spark.network.protocol.Message;

/**
 * Handles either request or response messages coming off of Netty. A MessageHandler instance
 * is associated with a single Netty Channel (though it may have multiple clients on the same
 * Channel.)
 * 处理Netty发出的请求或响应消息,MessageHandler实例与单个Netty通道相关联(尽管它可能在同一通道上具有多个客户端。)
 */
public abstract class MessageHandler<T extends Message> {
  /** Handles the receipt of a single message.
   * 处理单个消息的接收 */
  public abstract void handle(T message);

  /** Invoked when an exception was caught on the Channel.
   * 在频道上捕获到异常时调用 */
  public abstract void exceptionCaught(Throwable cause);

  /** Invoked when the channel this MessageHandler is on has been unregistered.
   * 当该MessageHandler所在的频道已被注销时调用 */
  public abstract void channelUnregistered();
}
