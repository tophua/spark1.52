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

package org.apache.spark.shuffle.unsafe

import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.Matchers

import org.apache.spark._
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer, Serializer}

/**
 * Tests for the fallback logic in UnsafeShuffleManager. Actual tests of shuffling data are
 * performed in other suites.
  * 测试UnsafeShuffleManager中的回退逻辑,洗牌数据的实际测试在其他套件中执行,
 */
class UnsafeShuffleManagerSuite extends SparkFunSuite with Matchers {

  import UnsafeShuffleManager.canUseUnsafeShuffle

  private class RuntimeExceptionAnswer extends Answer[Object] {
    override def answer(invocation: InvocationOnMock): Object = {
      throw new RuntimeException("Called non-stubbed method, " + invocation.getMethod.getName)
    }
  }

  private def shuffleDep(
      partitioner: Partitioner,
      serializer: Option[Serializer],
      keyOrdering: Option[Ordering[Any]],
      aggregator: Option[Aggregator[Any, Any, Any]],
      mapSideCombine: Boolean): ShuffleDependency[Any, Any, Any] = {//是否需要在worker端进行combine聚合操作
    val dep = mock(classOf[ShuffleDependency[Any, Any, Any]], new RuntimeExceptionAnswer())
    doReturn(0).when(dep).shuffleId
    doReturn(partitioner).when(dep).partitioner
    doReturn(serializer).when(dep).serializer
    doReturn(keyOrdering).when(dep).keyOrdering
    doReturn(aggregator).when(dep).aggregator
    doReturn(mapSideCombine).when(dep).mapSideCombine//是否需要在worker端进行combine聚合操作
    dep
  }

  test("supported shuffle dependencies") {//支持Shuffle依赖
    val kryo = Some(new KryoSerializer(new SparkConf()))

    assert(canUseUnsafeShuffle(shuffleDep(
      partitioner = new HashPartitioner(2),
      serializer = kryo,
      keyOrdering = None,
      aggregator = None,
      mapSideCombine = false//是否需要在worker端进行combine聚合操作
    )))

    val rangePartitioner = mock(classOf[RangePartitioner[Any, Any]])
    when(rangePartitioner.numPartitions).thenReturn(2)
    assert(canUseUnsafeShuffle(shuffleDep(
      partitioner = rangePartitioner,
      serializer = kryo,
      keyOrdering = None,
      aggregator = None,
      mapSideCombine = false//是否需要在worker端进行combine聚合操作
    )))

    // Shuffles with key orderings are supported as long as no aggregator is specified
    //Shuffles键的排序支持,只要没有指定聚合
    assert(canUseUnsafeShuffle(shuffleDep(
      partitioner = new HashPartitioner(2),
      serializer = kryo,
      keyOrdering = Some(mock(classOf[Ordering[Any]])),
      aggregator = None,//是否需要在worker端进行combine聚合操作
      mapSideCombine = false
    )))

  }

  test("unsupported shuffle dependencies") {//不支持Shuffle依赖
    val kryo = Some(new KryoSerializer(new SparkConf()))
    val java = Some(new JavaSerializer(new SparkConf()))

    // We only support serializers that support object relocation
    //我们只支持序列化支持对象重定位
    assert(!canUseUnsafeShuffle(shuffleDep(
      partitioner = new HashPartitioner(2),
      serializer = java,
      keyOrdering = None,
      aggregator = None,
      mapSideCombine = false//是否需要在worker端进行combine聚合操作
    )))

    // We do not support shuffles with more than 16 million output partitions
    //我们不支持将有超过1600万的输出分区
    assert(!canUseUnsafeShuffle(shuffleDep(
      partitioner = new HashPartitioner(UnsafeShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS + 1),
      serializer = kryo,
      keyOrdering = None,
      aggregator = None,
      mapSideCombine = false//是否需要在worker端进行combine聚合操作
    )))

    // We do not support shuffles that perform aggregation
    //我们不支持将执行聚合
    assert(!canUseUnsafeShuffle(shuffleDep(
      partitioner = new HashPartitioner(2),
      serializer = kryo,
      keyOrdering = None,
      aggregator = Some(mock(classOf[Aggregator[Any, Any, Any]])),
      mapSideCombine = false//是否需要在worker端进行combine聚合操作
    )))
    assert(!canUseUnsafeShuffle(shuffleDep(
      partitioner = new HashPartitioner(2),
      serializer = kryo,
      keyOrdering = Some(mock(classOf[Ordering[Any]])),
      aggregator = Some(mock(classOf[Aggregator[Any, Any, Any]])),
      mapSideCombine = true//是否需要在worker端进行combine聚合操作
    )))
  }

}
