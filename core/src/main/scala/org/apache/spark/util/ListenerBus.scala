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

import java.util.concurrent.CopyOnWriteArrayList

import scala.collection.JavaConversions._
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.spark.Logging
import org.apache.spark.scheduler.SparkListener

/**
 * An event bus which posts events to its listeners.
 * 一个事件总线,事件发送一个侦听器
 */
private[spark] trait ListenerBus[L <: AnyRef, E] extends Logging {

  // Marked `private[spark]` for access in tests.
  //CopyOnWriteArrayList是ArrayList的一个线程安全的变体
  private[spark] val listeners = new CopyOnWriteArrayList[L]

  /**
   * Add a listener to listen events. This method is thread-safe and can be called in any thread.
   * 添加一个监听事件,此方法是线程安全的,可以在任何线程中调用
   */
  final def addListener(listener: L) {
    listeners.add(listener)
  }

  /**
   * Post the event to all registered listeners. The `postToAll` caller should guarantee calling
   * `postToAll` in the same thread for all events.
    * 将事件发布到所有注册的侦听器,`postToAll`调用者应该保证在所有事件的同一个线程中调用`postToAll`。
   */
  final def postToAll(event: E): Unit = {
    // JavaConversions will create a JIterableWrapper if we use some Scala collection functions.
    // However, this method will be called frequently. To avoid the wrapper cost, here ewe use
    // Java Iterator directly.
    //如果我们使用一些Scala集合函数,JavaConversions将创建一个JIterableWrapper。
    // 然而，这种方法将被频繁调用。 为了避免包装成本,这里直接使用Java Iterator。
    val iter = listeners.iterator
    while (iter.hasNext) {
      val listener = iter.next()
      try {
        onPostEvent(listener, event)
      } catch {
        case NonFatal(e) =>
          logError(s"Listener ${Utils.getFormattedClassName(listener)} threw an exception", e)
      }
    }
  }

  /**
   * Post an event to the specified listener. `onPostEvent` is guaranteed to be called in the same
   * thread.
   * 将事件发送到指定的侦听器,onPostEvent保证是在同一个线程调用
   */
  def onPostEvent(listener: L, event: E): Unit

  private[spark] def findListenersByClass[T <: L : ClassTag](): Seq[T] = {
    val c = implicitly[ClassTag[T]].runtimeClass
    listeners.filter(_.getClass == c).map(_.asInstanceOf[T]).toSeq
  }

}
