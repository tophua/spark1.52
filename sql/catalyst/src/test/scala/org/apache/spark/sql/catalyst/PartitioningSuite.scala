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

package org.apache.spark.sql.catalyst

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{InterpretedMutableProjection, Literal}
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, HashPartitioning}

class PartitioningSuite extends SparkFunSuite {
  //HashPartitioning兼容性应该对表达式排序敏感
  test("HashPartitioning compatibility should be sensitive to expression ordering (SPARK-9785)") {
    val expressions = Seq(Literal(2), Literal(3))
    // Consider two HashPartitionings that have the same _set_ of hash expressions but which are
    // created with different orderings of those expressions:
    //考虑两个HashPartition,它们具有相同的散列表达式,但是它们是用不同顺序排列的那些表达式创建的：
    val partitioningA = HashPartitioning(expressions, 100)
    val partitioningB = HashPartitioning(expressions.reverse, 100)
    // These partitionings are not considered equal:
    //这些分区不被认为是相等的：
    assert(partitioningA != partitioningB)
    // However, they both satisfy the same clustered distribution:
    //但是,它们都满足相同的集群分布：
    val distribution = ClusteredDistribution(expressions)
    assert(partitioningA.satisfies(distribution))
    assert(partitioningB.satisfies(distribution))
    // These partitionings compute different hashcodes for the same input row:
    //这些分区为相同的输入行计算不同的哈希码:
    def computeHashCode(partitioning: HashPartitioning): Int = {
      val hashExprProj = new InterpretedMutableProjection(partitioning.expressions, Seq.empty)
      hashExprProj.apply(InternalRow.empty).hashCode()
    }
    assert(computeHashCode(partitioningA) != computeHashCode(partitioningB))
    // Thus, these partitionings are incompatible:
    //因此,这些分区是不兼容的：
    assert(!partitioningA.compatibleWith(partitioningB))
    assert(!partitioningB.compatibleWith(partitioningA))
    assert(!partitioningA.guarantees(partitioningB))
    assert(!partitioningB.guarantees(partitioningA))

    // Just to be sure that we haven't cheated by having these methods always return false,
    // check that identical partitionings are still compatible with and guarantee each other:
    assert(partitioningA === partitioningA)
    assert(partitioningA.guarantees(partitioningA))
    assert(partitioningA.compatibleWith(partitioningA))
  }
}
