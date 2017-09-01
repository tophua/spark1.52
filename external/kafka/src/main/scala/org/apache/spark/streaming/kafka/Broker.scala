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

package org.apache.spark.streaming.kafka

import org.apache.spark.annotation.Experimental

/**
 * Represents the host and port info for a Kafka broker.
  * 代表Kafka broker的主机和端口信息
 * Differs from the Kafka project's internal kafka.cluster.Broker, which contains a server ID.
  * 不同于Kafka项目的内部kafka.cluster.Broker,其中包含服务器ID
 */
final class Broker private(
    /** Broker's hostname */
    val host: String,
    /** Broker's port */
    val port: Int) extends Serializable {
  override def equals(obj: Any): Boolean = obj match {
    case that: Broker =>
      this.host == that.host &&
      this.port == that.port
    case _ => false
  }

  override def hashCode: Int = {
    41 * (41 + host.hashCode) + port
  }

  override def toString(): String = {
    s"Broker($host, $port)"
  }
}

/**
 * :: Experimental ::
 * Companion object that provides methods to create instances of [[Broker]].
  * Companion对象提供了创建[[Broker]]实例的方法
 */
@Experimental
object Broker {
  def create(host: String, port: Int): Broker =
    new Broker(host, port)

  def apply(host: String, port: Int): Broker =
    new Broker(host, port)

  def unapply(broker: Broker): Option[(String, Int)] = {
    if (broker == null) {
      None
    } else {
      Some((broker.host, broker.port))
    }
  }
}
