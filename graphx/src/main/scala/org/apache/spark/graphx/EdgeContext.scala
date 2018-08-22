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

package org.apache.spark.graphx

/**
 * Represents an edge along with its neighboring vertices and allows sending messages along the
 * edge. Used in [[Graph#aggregateMessages]].
  * 表示边缘及其相邻顶点,并允许沿边缘发送消息
 */
abstract class EdgeContext[VD, ED, A] {
  /** The vertex id of the edge's source vertex.
    * 边的源顶点的顶点id*/
  def srcId: VertexId
  /** The vertex id of the edge's destination vertex.
    * 边的目标顶点的顶点id*/
  def dstId: VertexId
  /** The vertex attribute of the edge's source vertex. 边的源顶点的顶点属性*/
  def srcAttr: VD
  /** The vertex attribute of the edge's destination vertex. 边的目标顶点的顶点属性*/
  def dstAttr: VD
  /** The attribute associated with the edge. 与边缘关联的属性*/
  def attr: ED

  /** Sends a message to the source vertex.将消息发送到源顶点 */
  def sendToSrc(msg: A): Unit
  /** Sends a message to the destination vertex. 将消息发送到目标顶点*/
  def sendToDst(msg: A): Unit

  /** Converts the edge and vertex properties into an [[EdgeTriplet]] for convenience.
    * 为方便起见,将edge和vertex属性转换为[[EdgeTriplet]] */
  def toEdgeTriplet: EdgeTriplet[VD, ED] = {
    val et = new EdgeTriplet[VD, ED]
    et.srcId = srcId
    et.srcAttr = srcAttr
    et.dstId = dstId
    et.dstAttr = dstAttr
    et.attr = attr
    et
  }
}

object EdgeContext {

  /**
   * Extractor mainly used for Graph#aggregateMessages*.
   * Example:
   * {{{
   *  val messages = graph.aggregateMessages(
   *    case ctx @ EdgeContext(_, _, _, _, attr) =>
   *      ctx.sendToDst(attr)
   *    , _ + _)
   * }}}
   */
  def unapply[VD, ED, A](edge: EdgeContext[VD, ED, A]): Some[(VertexId, VertexId, VD, VD, ED)] =
    Some(edge.srcId, edge.dstId, edge.srcAttr, edge.dstAttr, edge.attr)
}
