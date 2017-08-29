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
 * A simple listener for application events.
  * 一个应用程序事件的简单监听器
 *
 * This listener expects to hear events from a single application only. If events
 * from multiple applications are seen, the behavior is unspecified.
  * 这个听众只希望监听到来自单个应用程序的事件,如果看到来自多个应用程序的事件,则该行为是未指定的。
 */
private[spark] class ApplicationEventListener extends SparkListener {
  //None被声明为一个对象,而不是一个类,在没有值的时候,使用None,如果有值可以引用,就使用Some来包含这个值,都是Option的子类
  var appName: Option[String] = None
  var appId: Option[String] = None
  var appAttemptId: Option[String] = None
  var sparkUser: Option[String] = None
  var startTime: Option[Long] = None
  var endTime: Option[Long] = None
  var viewAcls: Option[String] = None
  var adminAcls: Option[String] = None

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart) {
    appName = Some(applicationStart.appName)
    appId = applicationStart.appId
    appAttemptId = applicationStart.appAttemptId
    startTime = Some(applicationStart.time)
    sparkUser = Some(applicationStart.sparkUser)
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
    endTime = Some(applicationEnd.time)
  }

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate) {
    synchronized {
      val environmentDetails = environmentUpdate.environmentDetails
      val allProperties = environmentDetails("Spark Properties").toMap
      //以逗号分隔Spark webUI访问用户的列表。默认情况下只有启动Spark job的用户才有访问权限
      viewAcls = allProperties.get("spark.ui.view.acls")
      adminAcls = allProperties.get("spark.admin.acls")
    }
  }
}
