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

package org.apache.spark.deploy.master

private[deploy] object DriverState extends Enumeration {

  type DriverState = Value

  // SUBMITTED: Submitted but not yet scheduled on a worker,提交但没有在Worker执行调度
  // RUNNING: Has been allocated to a worker to run,已经分配给一个Worker正在运行
  // FINISHED: Previously ran and exited cleanly 标记已经完成
  // RELAUNCHING: (重新启动)Exited non-zero or due to worker failure, but has not yet started running again
  // UNKNOWN: (由于主故障恢复,该驱动程序的状态暂时不知道)The state of the driver is temporarily not known due to master failure recovery
  // KILLED: A user manually killed this driver,一个用户手动杀死这个driver
  // FAILED:(失败) The driver exited non-zero and was not supervised,该驱动程序退出非零,并没有监督
  // ERROR: Unable to run or restart due to an unrecoverable error (e.g. missing jar file)(无法运行或重新启动,不可恢复的错误)
  val SUBMITTED, RUNNING, FINISHED, RELAUNCHING, UNKNOWN, KILLED, FAILED, ERROR = Value
}
