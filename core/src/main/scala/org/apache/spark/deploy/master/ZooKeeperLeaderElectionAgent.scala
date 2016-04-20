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

import org.apache.spark.{Logging, SparkConf}
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.leader.{LeaderLatchListener, LeaderLatch}
import org.apache.spark.deploy.SparkCuratorUtil

private[master] class ZooKeeperLeaderElectionAgent(val masterInstance: LeaderElectable,
    conf: SparkConf) extends LeaderLatchListener with LeaderElectionAgent with Logging  {

  val WORKING_DIR = conf.get("spark.deploy.zookeeper.dir", "/spark") + "/leader_election"

  private var zk: CuratorFramework = _
  private var leaderLatch: LeaderLatch = _
  private var status = LeadershipStatus.NOT_LEADER

  start()

  private def start() {
    logInfo("Starting ZooKeeper LeaderElection agent")
    zk = SparkCuratorUtil.newClient(conf)
    leaderLatch = new LeaderLatch(zk, WORKING_DIR)
    leaderLatch.addListener(this)//它实现LeanderLatchListener
    leaderLatch.start()//启动Lead的竞争与选举
  }

  override def stop() {
    leaderLatch.close()
    zk.close()
  }

  override def isLeader() {
    synchronized {
      // could have lost leadership by now.
      //有可能状态已经再次改变,即Leader再次变化,因此需要再次确认
      if (!leaderLatch.hasLeadership) {
        return
      }

      logInfo("We have gained leadership")
      //已经被选举Leader
      updateLeadershipStatus(true)
    }
  }

  override def notLeader() {
    synchronized {
      // could have gained leadership by now.
     //有可能状态已经再次改变,即Leader再次变化,因此需要再次确认
      if (leaderLatch.hasLeadership) {
        return
      }
      //被剥夺Leader
      logInfo("We have lost leadership")
      updateLeadershipStatus(false)
    }
  }

  private def updateLeadershipStatus(isLeader: Boolean) {
    if (isLeader && status == LeadershipStatus.NOT_LEADER) {
      status = LeadershipStatus.LEADER
      masterInstance.electedLeader()//Master已经被选举为Leader
    } else if (!isLeader && status == LeadershipStatus.LEADER) {
      status = LeadershipStatus.NOT_LEADER
      masterInstance.revokedLeadership()//Master已经被剥夺Leader
    }
  }

  private object LeadershipStatus extends Enumeration {
    type LeadershipStatus = Value
    val LEADER, NOT_LEADER = Value
  }
}
