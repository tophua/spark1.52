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

package org.apache.spark

import org.apache.spark.util.collection.OpenHashSet

/**
 * <span class="badge" style="float: right;">ALPHA COMPONENT</span>
 * GraphX is a graph processing framework built on top of Spark.
  * GraphX是一个建立在Spark之上的图形处理框架
 */
package object graphx {
  /**
   * A 64-bit vertex identifier that uniquely identifies a vertex within a graph. It does not need
   * to follow any ordering or any constraints other than uniqueness.
    * 64位顶点标识符,用于唯一标识图形中的顶点,除了唯一性之外,它不需要遵循任何排序或任何约束。
   */
  type VertexId = Long

  /** Integer identifer of a graph partition. Must be less than 2^30. */
  // TODO: Consider using Char.
  type PartitionID = Int

  private[graphx] type VertexSet = OpenHashSet[VertexId]
}
