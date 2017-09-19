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

package org.apache.spark.deploy

private[deploy] object ExecutorState extends Enumeration {
  /**
   * 定义Executor状态
   * LAUNCHING 启动,LOADING 加载,RUNNING运行,KILLED 杀死,FAILED失败,LOST丢失
   * EXITED退出
    * Value创建一个新的值,这是枚举的一部分
   */
  val LAUNCHING, LOADING, RUNNING, KILLED, FAILED, LOST, EXITED = Value
  //定义枚举类型别名
  type ExecutorState = Value

  def isFinished(state: ExecutorState): Boolean = Seq(KILLED, FAILED, LOST, EXITED).contains(state)
}
