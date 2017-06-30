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

// This file is placed in different package to make sure all of these components work well
// when they are outside of org.apache.spark.
package other.supplier

import java.nio.ByteBuffer

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.deploy.master._
import org.apache.spark.serializer.Serializer

class CustomRecoveryModeFactory(
  conf: SparkConf,
  serializer: Serializer
) extends StandaloneRecoveryModeFactory(conf, serializer) {

  CustomRecoveryModeFactory.instantiationAttempts += 1

  /**
   * PersistenceEngine defines how the persistent data(Information about worker, driver etc..)
   * is handled for recovery.
   * 持久性引擎定义了如何处理恢复持久数据
   *
   */
  override def createPersistenceEngine(): PersistenceEngine =
    new CustomPersistenceEngine(serializer)

  /**
   * Create an instance of LeaderAgent that decides who gets elected as master.
   * 创建代理的领导者决定谁当选的主人
   */
  override def createLeaderElectionAgent(master: LeaderElectable): LeaderElectionAgent =
    new CustomLeaderElectionAgent(master)
}

object CustomRecoveryModeFactory {
  @volatile var instantiationAttempts = 0
}

class CustomPersistenceEngine(serializer: Serializer) extends PersistenceEngine {
  //HashMap 创建方式
  val data = mutable.HashMap[String, Array[Byte]]()

  CustomPersistenceEngine.lastInstance = Some(this)//最后的实例

  /**
   * Defines how the object is serialized and persisted. Implementation will
   * depend on the store used.
   * 定义对象序列化持久化,实施将取决于所使用的存储
   */
  override def persist(name: String, obj: Object): Unit = {
    CustomPersistenceEngine.persistAttempts += 1
    val serialized = serializer.newInstance().serialize(obj)
    //创建Array对象,初始化数组长度
    val bytes = new Array[Byte](serialized.remaining())//返回剩余的可用长度
    serialized.get(bytes)
    //MAP追加方式(key:name,value:bytes)
    data += name -> bytes
  }

  /**
   * Defines how the object referred by its name is removed from the store.
   * 定义由它的名称所引用的对象如何从存储中删除
   */
  override def unpersist(name: String): Unit = {
    CustomPersistenceEngine.unpersistAttempts += 1
    data -= name
  }

  /**
   * Gives all objects, matching a prefix. This defines how objects are
   * read/deserialized back.
   * 给所有对象,匹配前缀,这一定义对象怎样读/反序列化回来
   */
  override def read[T: ClassTag](prefix: String): Seq[T] = {
    CustomPersistenceEngine.readAttempts += 1
    //map迭代方式
    val results = for ((name, bytes) <- data; if name.startsWith(prefix))
      yield serializer.newInstance().deserialize[T](ByteBuffer.wrap(bytes))
    results.toSeq
  }
}

object CustomPersistenceEngine {
  @volatile var persistAttempts = 0
  @volatile var unpersistAttempts = 0
  @volatile var readAttempts = 0

  @volatile var lastInstance: Option[CustomPersistenceEngine] = None //最后的实例
}

class CustomLeaderElectionAgent(val masterInstance: LeaderElectable) extends LeaderElectionAgent {
  masterInstance.electedLeader()
}

