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

/**
 *  "FAIR" and "FIFO" determines which policy is used
 *    to order tasks amongst a Schedulable's sub-queues
 *    “FAIR”和“FIFO”决定了哪个策略用于在Schedulable的子队列中排序任务
 *  "NONE" is used when the a Schedulable has no sub-queues.
  *  当“计划”没有子队列时，将使用“NONE”
 */
object SchedulingMode extends Enumeration {

  type SchedulingMode = Value
  val FAIR, FIFO, NONE = Value //声明一个调试模式枚举类型
}
