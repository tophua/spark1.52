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

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 *
 * A LeaderElectionAgent tracks current master and is a common interface for all election Agents.
  * LeaderElectionAgent跟踪当前主控，并且是所有选举代理的通用界面
 */
@DeveloperApi
trait LeaderElectionAgent {
  val masterInstance: LeaderElectable
  def stop() {} // to avoid noops in implementations.
}

@DeveloperApi
trait LeaderElectable {
  def electedLeader()
  def revokedLeadership()
}

/** Single-node implementation of LeaderElectionAgent -- we're initially and always the leader.
  * LeaderElectionAgent的单节点实现 - 我们最初始终是领导者 */
private[spark] class MonarchyLeaderAgent(val masterInstance: LeaderElectable)
  extends LeaderElectionAgent {
  masterInstance.electedLeader()
}
