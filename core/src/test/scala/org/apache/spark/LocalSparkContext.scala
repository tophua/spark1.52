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

package org.apache.spark

import _root_.io.netty.util.internal.logging.{Slf4JLoggerFactory, InternalLoggerFactory}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Suite

/** 
 *  Manages a local `sc` {@link SparkContext} variable, correctly stopping it after each test. 
 *  管理一个本地"SC"变量,测试后停止。
 *  */
trait LocalSparkContext extends BeforeAndAfterEach with BeforeAndAfterAll { 
  /**
   * 特质可以要求混入它的类扩展自另一个类型，但是当使用自身类型（self type）的声明来定义特质时（this: ClassName =>），
   * 这样的特质只能被混入给定类型的子类当中,
   */     
  self: Suite =>
  //transient注解用于标记变量不被序列化
  @transient var sc: SparkContext = _

  override def beforeAll() {
    //内部工厂日志器
    InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory())
    super.beforeAll()
  }

  override def afterEach() {
    resetSparkContext()
    super.afterEach()
  }

  def resetSparkContext(): Unit = {
    LocalSparkContext.stop(sc)
    sc = null
  }

}

object LocalSparkContext {
  def stop(sc: SparkContext) {
    if (sc != null) {
      sc.stop()
    }
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    //为了避免Akka重新绑定到相同的端口,不绑定立即关机
    System.clearProperty("spark.driver.port")
  }

  /**
   * Runs `f` by passing in `sc` and ensures(确保) that `sc` is stopped.
   * 柯里化（Currying）是把接受多个参数的函数变换成接受一个单一参数(最初函数的第一个参数)的函数，
   * 并且返回接受余下的参数且返回结果
   **/
  def withSpark[T](sc: SparkContext)(f: SparkContext => T): T = {
    try {
      f(sc)
    } finally {
      stop(sc)
    }
  }

}
