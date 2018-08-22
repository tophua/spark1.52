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

import org.scalatest.{BeforeAndAfterEach, Matchers}

import org.apache.spark.SparkFunSuite
//容器布局策略测试
class ContainerPlacementStrategySuite extends SparkFunSuite with Matchers with BeforeAndAfterEach {

  private val yarnAllocatorSuite = new YarnAllocatorSuite
  import yarnAllocatorSuite._

  override def beforeEach() {
    yarnAllocatorSuite.beforeEach()
  }

  override def afterEach() {
    yarnAllocatorSuite.afterEach()
  }
  //分配具有足够资源的本地偏好容器,并且没有匹配的存储容器
  test("allocate locality preferred containers with enough resource and no matched existed " +
    "containers") {
    // 1. All the locations of current containers cannot satisfy the new requirements
    // 1.当前容器的所有位置都不能满足新的要求
    // 2. Current requested container number can fully satisfy the pending tasks.
    // 2.当前请求的容器号可以完全满足挂起的任务

    val handler = createAllocator(2)
    handler.updateResourceRequests()
    handler.handleAllocatedContainers(Array(createContainer("host1"), createContainer("host2")))

    val localities = handler.containerPlacementStrategy.localityOfRequestedContainers(
      3, 15, Map("host3" -> 15, "host4" -> 15, "host5" -> 10), handler.allocatedHostToContainersMap)

    assert(localities.map(_.nodes) === Array(
      Array("host3", "host4", "host5"),
      Array("host3", "host4", "host5"),
      Array("host3", "host4")))
  }
  //分配具有足够资源和部分匹配的容器的地方首选容器
  test("allocate locality preferred containers with enough resource and partially matched " +
    "containers") {
    // 1. Parts of current containers' locations can satisfy the new requirements
    //1. 当前容器的部分位置可以满足新的要求
    // 2. Current requested container number can fully satisfy the pending tasks.
    //2.当前请求的容器编号可以完全满足待处理的任务
    val handler = createAllocator(3)
    handler.updateResourceRequests()
    handler.handleAllocatedContainers(Array(
      createContainer("host1"),
      createContainer("host1"),
      createContainer("host2")
    ))

    val localities = handler.containerPlacementStrategy.localityOfRequestedContainers(
      3, 15, Map("host1" -> 15, "host2" -> 15, "host3" -> 10), handler.allocatedHostToContainersMap)

    assert(localities.map(_.nodes) ===
      Array(null, Array("host2", "host3"), Array("host2", "host3")))
  }
  //分配有限资源和部分匹配的容器的地方首选容器
  test("allocate locality preferred containers with limited resource and partially matched " +
    "containers") {
    // 1. Parts of current containers' locations can satisfy the new requirements
    //当前集装箱的部分位置可以满足新的要求
    // 2. Current requested container number cannot fully satisfy the pending tasks.
    //当前请求的容器编号不能完全满足待处理的任务。

    val handler = createAllocator(3)
    handler.updateResourceRequests()
    handler.handleAllocatedContainers(Array(
      createContainer("host1"),
      createContainer("host1"),
      createContainer("host2")
    ))

    val localities = handler.containerPlacementStrategy.localityOfRequestedContainers(
      1, 15, Map("host1" -> 15, "host2" -> 15, "host3" -> 10), handler.allocatedHostToContainersMap)

    assert(localities.map(_.nodes) === Array(Array("host2", "host3")))
  }
  //分配具有完全匹配的容器的地方首选容器
  test("allocate locality preferred containers with fully matched containers") {
    // Current containers' locations can fully satisfy the new requirements
    //当前集装箱的位置可以完全满足新的要求
    val handler = createAllocator(5)
    handler.updateResourceRequests()
    handler.handleAllocatedContainers(Array(
      createContainer("host1"),
      createContainer("host1"),
      createContainer("host2"),
      createContainer("host2"),
      createContainer("host3")
    ))

    val localities = handler.containerPlacementStrategy.localityOfRequestedContainers(
      3, 15, Map("host1" -> 15, "host2" -> 15, "host3" -> 10), handler.allocatedHostToContainersMap)

    assert(localities.map(_.nodes) === Array(null, null, null))
  }
  //分配没有本地偏好的容器
  test("allocate containers with no locality preference") {
    // Request new container without locality preference
    //请求没有本地偏好的新容器
    val handler = createAllocator(2)
    handler.updateResourceRequests()
    handler.handleAllocatedContainers(Array(createContainer("host1"), createContainer("host2")))

    val localities = handler.containerPlacementStrategy.localityOfRequestedContainers(
      1, 0, Map.empty, handler.allocatedHostToContainersMap)

    assert(localities.map(_.nodes) === Array(null))
  }
}
