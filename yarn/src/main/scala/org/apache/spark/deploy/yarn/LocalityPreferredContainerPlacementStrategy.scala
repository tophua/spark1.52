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

package org.apache.spark.deploy.yarn

import scala.collection.mutable.{ArrayBuffer, HashMap, Set}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.records.{ContainerId, Resource}
import org.apache.hadoop.yarn.util.RackResolver

import org.apache.spark.SparkConf

private[yarn] case class ContainerLocalityPreferences(nodes: Array[String], racks: Array[String])

/**
 * This strategy is calculating the optimal locality preferences of YARN containers by considering
 * the node ratio of pending tasks, number of required cores/containers and and locality of current
 * existing containers. The target of this algorithm is to maximize the number of tasks that
 * would run locally.
 *此策略通过考虑待处理任务的节点比率,所需核心/容器的数量以及当前现有容器的位置来计算YARN容器的最佳位置偏好。 该算法的目标是最大化本地运行的任务数量。
  *
 * Consider a situation in which we have 20 tasks that require (host1, host2, host3)
 * and 10 tasks that require (host1, host2, host4), besides each container has 2 cores
 * and cpus per task is 1, so the required container number is 15,
 * and host ratio is (host1: 30, host2: 30, host3: 20, host4: 10).
 *
  * 考虑一种情况,我们需要20个任务（host1，host2，host3）和10个需要的任务(host1，host2，host4),
  * 此外每个容器有2个核心,每个任务的cpus为1,所以所需的容器号是 15，
  * 宿主比例为（host1：30，host2：30，host3：20，host4：10）。
  *
 * 1. If requested container number (18) is more than the required container number (15):
 *
 * requests for 5 containers with nodes: (host1, host2, host3, host4)
 * requests for 5 containers with nodes: (host1, host2, host3)
 * requests for 5 containers with nodes: (host1, host2)
 * requests for 3 containers with no locality preferences.
 *
 * The placement ratio is 3 : 3 : 2 : 1, and set the additional containers with no locality
 * preferences.
 *
 * 2. If requested container number (10) is less than or equal to the required container number
 * (15):
 *
 * requests for 4 containers with nodes: (host1, host2, host3, host4)
 * requests for 3 containers with nodes: (host1, host2, host3)
 * requests for 3 containers with nodes: (host1, host2)
 *
 * The placement ratio is 10 : 10 : 7 : 4, close to expected ratio (3 : 3 : 2 : 1)
 *
 * 3. If containers exist but none of them can match the requested localities,
 * follow the method of 1 and 2.
 *
 * 4. If containers exist and some of them can match the requested localities.
 * For example if we have 1 containers on each node (host1: 1, host2: 1: host3: 1, host4: 1),
 * and the expected containers on each node would be (host1: 5, host2: 5, host3: 4, host4: 2),
 * so the newly requested containers on each node would be updated to (host1: 4, host2: 4,
 * host3: 3, host4: 1), 12 containers by total.
 *
 *   4.1 If requested container number (18) is more than newly required containers (12). Follow
 *   method 1 with updated ratio 4 : 4 : 3 : 1.
 *
 *   4.2 If request container number (10) is more than newly required containers (12). Follow
 *   method 2 with updated ratio 4 : 4 : 3 : 1.
 *
 * 5. If containers exist and existing localities can fully cover the requested localities.
 * For example if we have 5 containers on each node (host1: 5, host2: 5, host3: 5, host4: 5),
 * which could cover the current requested localities. This algorithm will allocate all the
 * requested containers with no localities.
 */
private[yarn] class LocalityPreferredContainerPlacementStrategy(
    val sparkConf: SparkConf,
    val yarnConf: Configuration,
    val resource: Resource) {

  // Number of CPUs per task 每个任务的CPU数量
  private val CPUS_PER_TASK = sparkConf.getInt("spark.task.cpus", 1)

  /**
   * Calculate each container's node locality and rack locality
    * 计算每个容器的节点位置和机架位置
   * @param numContainer number of containers to calculate 要计算的容器数量
   * @param numLocalityAwareTasks number of locality required tasks 所需的任务数量
   * @param hostToLocalTaskCount a map to store the preferred hostname and possible task
   *                             numbers running on it, used as hints for container allocation
    *                             用于存储首选主机名和在其上运行的可能任务编号的映射,用作容器分配的提示
   * @return node localities and rack localities, each locality is an array of string,
   *         the length of localities is the same as number of containers
    *         节点局部性和机架局部性,每个局部是一个字符串数组,局部长度与容器数相同
   */
  def localityOfRequestedContainers(
      numContainer: Int,
      numLocalityAwareTasks: Int,
      hostToLocalTaskCount: Map[String, Int],
      allocatedHostToContainersMap: HashMap[String, Set[ContainerId]]
    ): Array[ContainerLocalityPreferences] = {
    val updatedHostToContainerCount = expectedHostToContainerCount(
      numLocalityAwareTasks, hostToLocalTaskCount, allocatedHostToContainersMap)
    val updatedLocalityAwareContainerNum = updatedHostToContainerCount.values.sum

    // The number of containers to allocate, divided into two groups, one with preferred locality,
    // and the other without locality preference.
    //要分配的容器数量,分为两组,一组具有首选位置,另一组没有位置首选项
    val requiredLocalityFreeContainerNum =
      math.max(0, numContainer - updatedLocalityAwareContainerNum)
    val requiredLocalityAwareContainerNum = numContainer - requiredLocalityFreeContainerNum

    val containerLocalityPreferences = ArrayBuffer[ContainerLocalityPreferences]()
    if (requiredLocalityFreeContainerNum > 0) {
      for (i <- 0 until requiredLocalityFreeContainerNum) {
        containerLocalityPreferences += ContainerLocalityPreferences(
          null.asInstanceOf[Array[String]], null.asInstanceOf[Array[String]])
      }
    }

    if (requiredLocalityAwareContainerNum > 0) {
      val largestRatio = updatedHostToContainerCount.values.max
      // Round the ratio of preferred locality to the number of locality required container
      // number, which is used for locality preferred host calculating.
      //将首选地点的比率与所需地点数量的容器数量相对应,该数量用于地点首选主机计算
      var preferredLocalityRatio = updatedHostToContainerCount.mapValues { ratio =>
        val adjustedRatio = ratio.toDouble * requiredLocalityAwareContainerNum / largestRatio
        adjustedRatio.ceil.toInt
      }

      for (i <- 0 until requiredLocalityAwareContainerNum) {
        // Only filter out the ratio which is larger than 0, which means the current host can
        // still be allocated with new container request.
        //仅筛选出大于0的比率,这意味着仍可以使用新的容器请求分配当前主机。
        val hosts = preferredLocalityRatio.filter(_._2 > 0).keys.toArray
        val racks = hosts.map { h =>
          RackResolver.resolve(yarnConf, h).getNetworkLocation
        }.toSet
        containerLocalityPreferences += ContainerLocalityPreferences(hosts, racks.toArray)

        // Minus 1 each time when the host is used. When the current ratio is 0,
        // which means all the required ratio is satisfied, this host will not be allocated again.
        //每次使用主机时减1,当当前比率为0时,表示满足所有要求的比率,将不再分配该主机。
        preferredLocalityRatio = preferredLocalityRatio.mapValues(_ - 1)
      }
    }

    containerLocalityPreferences.toArray
  }

  /**
   * Calculate the number of executors need to satisfy the given number of pending tasks.
    * 计算需要满足给定挂起任务数的执行程序数
   */
  private def numExecutorsPending(numTasksPending: Int): Int = {
    val coresPerExecutor = resource.getVirtualCores
    (numTasksPending * CPUS_PER_TASK + coresPerExecutor - 1) / coresPerExecutor
  }

  /**
   * Calculate the expected host to number of containers by considering with allocated containers.
    * 通过考虑分配的容器来计算预期的主机到容器数量
   * @param localityAwareTasks number of locality aware tasks 地点感知任务的数量
   * @param hostToLocalTaskCount a map to store the preferred hostname and possible task
   *                             numbers running on it, used as hints for container allocation
    *                             用于存储首选主机名和在其上运行的可能任务编号的映射,用作容器分配的提示
   * @return a map with hostname as key and required number of containers on this host as value
    *         主机名为键的映射,以及此主机上所需的容器数作为值
   */
  private def expectedHostToContainerCount(
      localityAwareTasks: Int,
      hostToLocalTaskCount: Map[String, Int],
      allocatedHostToContainersMap: HashMap[String, Set[ContainerId]]
    ): Map[String, Int] = {
    val totalLocalTaskNum = hostToLocalTaskCount.values.sum
    hostToLocalTaskCount.map { case (host, count) =>
      val expectedCount =
        count.toDouble * numExecutorsPending(localityAwareTasks) / totalLocalTaskNum
      val existedCount = allocatedHostToContainersMap.get(host)
        .map(_.size)
        .getOrElse(0)

      // If existing container can not fully satisfy the expected number of container,
      //如果现有容器不能完全满足预期的容器数量
      // the required container number is expected count minus existed count. Otherwise the
      // required container number is 0.
      //所需的容器编号是预期的数量减去已存在的数量,否则,所需的容器编号为0
      (host, math.max(0, (expectedCount - existedCount).ceil.toInt))
    }
  }
}
