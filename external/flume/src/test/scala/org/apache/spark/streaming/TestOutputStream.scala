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

package org.apache.spark.streaming

import java.io.{IOException, ObjectInputStream}

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ForEachDStream}
import org.apache.spark.util.Utils

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * This is a output stream just for the testsuites. All the output is collected into a
 * ArrayBuffer. This buffer is wiped clean on being restored from checkpoint.
 * 这只是一个输出流的测试包,所有的输出被收集成一个数组缓存,从检查点恢复时,该缓冲区被清除干净
 * The buffer contains a sequence of RDD's, each containing a sequence of items
 * 缓冲区包含一个序列的RDD,每个包含一个项目序列
 */
class TestOutputStream[T: ClassTag](parent: DStream[T],
    val output: ArrayBuffer[Seq[T]] = ArrayBuffer[Seq[T]]())
  extends ForEachDStream[T](parent, (rdd: RDD[T], t: Time) => {
    val collected = rdd.collect()
    output += collected
  }) {

  // This is to clear the output buffer every it is read from a checkpoint
  //这是清除输出缓冲区,它是从一个检查点读取
  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream): Unit = Utils.tryOrIOException {
    ois.defaultReadObject()
    output.clear()
  }
}
