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

import io.netty.channel.Channel;

/**
 * A bootstrap which is executed on a TransportClient before it is returned to the user.
 * 一个引导程序,它在传回客户端返回给用户之前执行
 * This enables an initial exchange of information (e.g., SASL authentication tokens) on a once-per-
 * connection basis.
 * 这使得能够在每次连接的基础上初始交换信息(例如，SASL认证令牌)
 *
 * Since connections (and TransportClients) are reused as much as possible, it is generally
 * reasonable to perform an expensive bootstrapping operation, as they often share a lifespan with
 * the JVM itself.
 * 由于连接(和TransportClients)尽可能重用,所以执行昂贵的引导操作通常是合理的,因为它们通常与JVM本身共享寿命
 */
public interface TransportClientBootstrap {
  /** Performs the bootstrapping operation, throwing an exception on failure.
   * 执行引导操作,在失败时抛出异常。*/
  void doBootstrap(TransportClient client, Channel channel) throws RuntimeException;
}
