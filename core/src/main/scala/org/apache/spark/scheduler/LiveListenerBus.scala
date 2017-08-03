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

package org.apache.spark.scheduler

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.util.AsynchronousListenerBus

/**
 * Asynchronously passes SparkListenerEvents to registered SparkListeners.
  * 将SparkListenerEvent异步传递给注册的SparkListener
  *
 * Until start() is called, all posted events are only buffered. Only after this listener bus
 * has started will events be actually propagated to all attached listeners. This listener bus
 * is stopped when it receives a SparkListenerShutdown event, which is posted using stop().
  * 直到start（）被调用所有发布的事件才被缓存,只有在这个听众总线开始之后,事件才能实际传播到所有附加的监听,
  * 当收到一个SparkListenerShutdown事件时,该监听器总线被停止,该事件使用stop()发布。
 */
private[spark] class LiveListenerBus
  //事件阻塞队列,先进先出
  extends AsynchronousListenerBus[SparkListener, SparkListenerEvent]("SparkListenerBus")
  with SparkListenerBus {

  private val logDroppedEvent = new AtomicBoolean(false)

  override def onDropEvent(event: SparkListenerEvent): Unit = {
    if (logDroppedEvent.compareAndSet(false, true)) {
      // Only log the following message once to avoid duplicated annoying logs.
      //只能记录一次以下消息,以避免重复的烦人日志,
      logError("Dropping SparkListenerEvent because no remaining room in event queue. " +
        "This likely means one of the SparkListeners is too slow and cannot keep up with " +
        "the rate at which tasks are being started by the scheduler.")
    }
  }

}
