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

package org.apache.spark.util

import java.util.concurrent.atomic.AtomicInteger

/**
 * A util used to get a unique generation ID. This is a wrapper around Java's
 * AtomicInteger. An example usage is in BlockManager, where each BlockManager
 * instance would start an RpcEndpoint and we use this utility to assign the RpcEndpoints'
 * unique names.
  * 用于获取唯一生成ID的实用程序,这是Java的AtomicInteger的包装,BlockManager中的一个示例用法，
  * 其中每个BlockManager实例将启动一个RpcEndpoint,我们使用此实用程序分配RpcEndpoints的唯一名称。
 */
private[spark] class IdGenerator {
  private val id = new AtomicInteger
  def next: Int = id.incrementAndGet
}
