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

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{BlockingQueue, LinkedBlockingDeque}

import scala.util.control.NonFatal

import org.apache.spark.Logging

/**
 * EventLoop里面开辟了一个线程,这个线程不断的循环队列,post的时候其实就是将消息放入到这个队列里面,
 * 由于线程不断循环,因此放到队列里面可以拿到,拿到后就会回调DAGSchedulerEventProcessLoop里面的onReceive,
 * 处理的时候onReceive调用doOnReceive。
 * An event loop to receive events from the caller and process all events in the event thread. It
 * will start an exclusive event thread to process all events.
 * EventLoop用来接收来自调用者的事件并在event thread中除了所有的事件。它将开启一个专门的事件处理线程处理所有的事件。 
 * Note: The event queue will grow indefinitely. So subclasses should make sure `onReceive` can
 * handle events in time to avoid the potential OOM.
 */
private[spark] abstract class EventLoop[E](name: String) extends Logging {
  //一个由链表结构组成的有界阻塞队列,此队列按照先进先出的原则对元素进行排序
  private val eventQueue: BlockingQueue[E] = new LinkedBlockingDeque[E]()
  //是否停止标志位 
  private val stopped = new AtomicBoolean(false)
  //事件处理线程  
  private val eventThread = new Thread(name) {
    //setDaemon(true)是守护线程,为啥是守护线程呢？
    //作为后台线程,在后台不断的循环,如果是前台线程的话,对垃圾的回收是有影响的
    setDaemon(true)

    override def run(): Unit = {
      try {
        //果标志位stopped没有被设置为true,一直循环 
        while (!stopped.get) {
         //从eventQueue中获得消息队列
          val event = eventQueue.take()
          try {
             //调用onReceive()方法进行处理  
            onReceive(event)//接收消息。在这里并没有直接实现OnReceive方法,
                            //具体方法实现是在DAGScheduler#onReceive
          } catch {
            case NonFatal(e) => {
              try {
                onError(e)
              } catch {
                case NonFatal(e) => logError("Unexpected error in " + name, e)
              }
            }
          }
        }
      } catch {
        case ie: InterruptedException => // exit even if eventQueue is not empty
        case NonFatal(e) => logError("Unexpected error in " + name, e)
      }
    }

  }

  def start(): Unit = {
    if (stopped.get) {
      throw new IllegalStateException(name + " has already been stopped")
    }
    // Call onStart before starting the event thread to make sure it happens before onReceive
    //在启动事件线程之前调用onStart,以确保在onReceive之前发生
    onStart()
    eventThread.start()
  }

  def stop(): Unit = {
    //compareAndSet 如果当前值与期望值(第一个参数)相等,则设置为新值(第二个参数),设置成功返回true
    if (stopped.compareAndSet(false, true)) {
      eventThread.interrupt()//
      var onStopCalled = false
      try {
        eventThread.join()
        // Call onStop after the event thread exits to make sure onReceive happens before onStop
        //在事件线程退出后调用onStop,以确保onReceive在onStop之前发生
        onStopCalled = true
        onStop()
      } catch {
        case ie: InterruptedException =>
          //interrupt不会中断一个正在运行的线程。它的作用是,在线程受到阻塞时抛出一个中断信号,这样线程就得以退出阻塞的状态.
          Thread.currentThread().interrupt()
          if (!onStopCalled) {
            // ie is thrown from `eventThread.join()`. Otherwise, we should not call `onStop` since
            // it's already called.
            //即从`eventThread.join（）`抛出, 否则,我们不应该调用`onStop',因为它已经被调用了。
            onStop()
          }
      }
    } else {
      // Keep quiet to allow calling `stop` multiple times.
      //保持安静,允许多次调用`stop`。
    }
  }

  /**
   * Put the event into the event queue. The event thread will process it later.
   * 将事件加入到时间队列。事件线程过会会处理它。 
   */
  def post(event: E): Unit = {
     //将事件加入到待处理队列  
    eventQueue.put(event)
  }

  /**
   * Return if the event thread has already been started but not yet stopped.
    * 如果事件线程已经启动但尚未停止,则返回。
   */
  def isActive: Boolean = eventThread.isAlive

  /**
   * Invoked when `start()` is called but before the event thread starts.
    * 当`start（）`被调用但事件线程启动之前调用
   */
  protected def onStart(): Unit = {}

  /**
   * Invoked when `stop()` is called and the event thread exits.
    * 调用`stop（）`并且事件线程退出时调用
   */
  protected def onStop(): Unit = {}

  /**
   * Invoked in the event thread when polling events from the event queue.
    * 在事件队列中轮询事件时调用事件线程
   *
   * Note: Should avoid calling blocking actions in `onReceive`, or the event thread will be blocked
   * and cannot process events in time. If you want to call some blocking actions, run them in
   * another thread.
    * 注意：应避免在onReceive'中调用阻塞操作,否则事件线程将被阻塞,无法及时处理事件。
    * 如果要调用某些阻止操作，请在另一个线程中运行它们。
   */
  protected def onReceive(event: E): Unit

  /**
   * Invoked if `onReceive` throws any non fatal error. Any non fatal error thrown from `onError`
   * will be ignored.
    * 如果`onReceive`抛出任何非致命错误，则调用它。 从`onError`抛出的任何非致命错误将被忽略。
   */
  protected def onError(e: Throwable): Unit

}
