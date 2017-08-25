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

package org.apache.spark.storage

import java.util.UUID

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * Identifies a particular Block of data, usually associated with a single file.
 * A Block can be uniquely identified by its filename, but each type of Block has a different
 * set of keys which produce its unique name.
 * 一个数据块可以通过其文件名的唯一标识,但每一种类型的块设置有不同的Key集合,产生其唯一的名称
 * If your BlockId should be serializable, be sure to add it to the BlockId.apply() method.
  * 如果你的BlockId可以序列化,请确保将其添加到BlockId.apply()方法中
  * RDD的每个partition分区对应Storage模块的一个Block数据块,只不过Block是Partition经过处理后的数据
 */
@DeveloperApi
sealed abstract class BlockId {
  /** A globally unique identifier for this Block. Can be used for ser/de. */
  //此块的全局唯一标识符,可用于ser/de
  def name: String

  // convenience methods
  //asInstanceOf强制类型转换
  def asRDDId: Option[RDDBlockId] = if (isRDD) Some(asInstanceOf[RDDBlockId]) else None
  //isInstanceOf和asInstanceOf 由scala.Any类定义,Scala类层级的根类,
  // 所有对象都自动拥有isInstanceOf和asInstanceOf
  def isRDD: Boolean = isInstanceOf[RDDBlockId]//判断对象是否为RDDBlockId类型的实例
  def isShuffle: Boolean = isInstanceOf[ShuffleBlockId]//判断一个对象实例是否是某种类型
  def isBroadcast: Boolean = isInstanceOf[BroadcastBlockId]

  override def toString: String = name
  override def hashCode: Int = name.hashCode
  override def equals(other: Any): Boolean = other match {
    case o: BlockId => getClass == o.getClass && name.equals(o.name)
    case _ => false
  }
}

@DeveloperApi
case class RDDBlockId(rddId: Int, splitIndex: Int) extends BlockId {
  override def name: String = "rdd_" + rddId + "_" + splitIndex
}

// Format of the shuffle block ids (including data and index) should be kept in sync with
// 格式shuffle 块ids(包括数据和索引)应保持同步
// org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getBlockData().
@DeveloperApi
case class ShuffleBlockId(shuffleId: Int, mapId: Int, reduceId: Int) extends BlockId {
  //ShuffleBlockId的格式如下,//mapId对应RDD的partionsID
  override def name: String = "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId
}

@DeveloperApi
//mapId对应RDD的partionsID
case class ShuffleDataBlockId(shuffleId: Int, mapId: Int, reduceId: Int) extends BlockId {
  override def name: String = "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId + ".data"
}

@DeveloperApi
//mapId对应RDD的partionsID
case class ShuffleIndexBlockId(shuffleId: Int, mapId: Int, reduceId: Int) extends BlockId {
  override def name: String = "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId + ".index"
}

@DeveloperApi
case class BroadcastBlockId(broadcastId: Long, field: String = "") extends BlockId {
  override def name: String = "broadcast_" + broadcastId + (if (field == "") "" else "_" + field)
}

@DeveloperApi
case class TaskResultBlockId(taskId: Long) extends BlockId {
  override def name: String = "taskresult_" + taskId
}

@DeveloperApi
case class StreamBlockId(streamId: Int, uniqueId: Long) extends BlockId {
  override def name: String = "input-" + streamId + "-" + uniqueId
}

/** Id associated with temporary local data managed as blocks. Not serializable.
  * 与作为块管理的临时本地数据相关联的Id。 不可序列化 */
private[spark] case class TempLocalBlockId(id: UUID) extends BlockId {
  override def name: String = "temp_local_" + id
}

/** 
 *  Id associated with temporary shuffle data managed as blocks. Not serializable. 
 *  与作为块管理的临时shuffle数据相关联的Id,不可序列化
 *  */
private[spark] case class TempShuffleBlockId(id: UUID) extends BlockId {
  override def name: String = "temp_shuffle_" + id
}

// Intended only for testing purposes 仅用于测试目的
private[spark] case class TestBlockId(id: String) extends BlockId {
  override def name: String = "test_" + id
}

@DeveloperApi
object BlockId {
  val RDD = "rdd_([0-9]+)_([0-9]+)".r //返回正则表达式
  val SHUFFLE = "shuffle_([0-9]+)_([0-9]+)_([0-9]+)".r
  val SHUFFLE_DATA = "shuffle_([0-9]+)_([0-9]+)_([0-9]+).data".r
  val SHUFFLE_INDEX = "shuffle_([0-9]+)_([0-9]+)_([0-9]+).index".r
  val BROADCAST = "broadcast_([0-9]+)([_A-Za-z0-9]*)".r
  val TASKRESULT = "taskresult_([0-9]+)".r
  val STREAM = "input-([0-9]+)-([0-9]+)".r
  val TEST = "test_(.*)".r

  /** 
   *  Converts a BlockId "name" String back into a BlockId.
    *  将BlockId“name”字符串转换回BlockId
   */
  def apply(id: String): BlockId = id match {
    case RDD(rddId, splitIndex) =>
      RDDBlockId(rddId.toInt, splitIndex.toInt)
    //mapId对应RDD的partionsID
    case SHUFFLE(shuffleId, mapId, reduceId) =>
      //mapId对应RDD的partionsID
      ShuffleBlockId(shuffleId.toInt, mapId.toInt, reduceId.toInt)
    //mapId对应RDD的partionsID
    case SHUFFLE_DATA(shuffleId, mapId, reduceId) =>
      //mapId对应RDD的partionsID
      ShuffleDataBlockId(shuffleId.toInt, mapId.toInt, reduceId.toInt)
    //mapId对应RDD的partionsID
    case SHUFFLE_INDEX(shuffleId, mapId, reduceId) =>
      ShuffleIndexBlockId(shuffleId.toInt, mapId.toInt, reduceId.toInt)
    case BROADCAST(broadcastId, field) =>
      BroadcastBlockId(broadcastId.toLong, field.stripPrefix("_"))
    case TASKRESULT(taskId) =>
      TaskResultBlockId(taskId.toLong)
    case STREAM(streamId, uniqueId) =>
      StreamBlockId(streamId.toInt, uniqueId.toLong)
    case TEST(value) =>
      TestBlockId(value)
    case _ =>
      throw new IllegalStateException("Unrecognized BlockId: " + id)
  }
}
