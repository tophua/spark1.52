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

package org.apache.spark.broadcast

import scala.reflect.ClassTag

import org.apache.spark.{SecurityManager, SparkConf}

/**
 * A [[org.apache.spark.broadcast.Broadcast]] implementation that uses a BitTorrent-like
 * protocol to do a distributed transfer of the broadcasted data to the executors. Refer to
 * [[org.apache.spark.broadcast.TorrentBroadcast]] for more details.
  * A [[org.apache.spark.broadcast.Broadcast]]实现使用BitTorrent类协议将广播的数据分发传输给执行者。
  * 参考[[org.apache.spark.broadcast.TorrentBroadcast]]更多细节
 */
class TorrentBroadcastFactory extends BroadcastFactory {

  override def initialize(isDriver: Boolean, conf: SparkConf, securityMgr: SecurityManager) { }
  //创建一个TorrentBroadcast对象
  override def newBroadcast[T: ClassTag](value_ : T, isLocal: Boolean, id: Long): Broadcast[T] = {
    new TorrentBroadcast[T](value_, id)
  }

  override def stop() { }

  /**
   * Remove all persisted state associated with the torrent broadcast with the given ID.
    * 删除与给定ID的洪流广播相关联的所有持久状态
   * @param removeFromDriver Whether to remove state from the driver.
   * @param blocking Whether to block until unbroadcasted
   */
  override def unbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean) {
    TorrentBroadcast.unpersist(id, removeFromDriver, blocking)
  }
}
