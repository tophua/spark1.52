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

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.hadoop.fs.Path

import org.apache.spark._
import org.apache.spark.util.SerializableConfiguration

/**
 * An implementation of checkpointing that writes the RDD data to reliable storage.
  * 将RDD数据写入可靠存储的检查点的实现
 * This allows drivers to be restarted on failure with previously computed state.
  * 这允许在以前计算的状态下,在故障时重新启动驱动程序
 */
private[spark] class ReliableRDDCheckpointData[T: ClassTag](@transient rdd: RDD[T])
  extends RDDCheckpointData[T](rdd) with Logging {

  // The directory to which the associated RDD has been checkpointed to
  // This is assumed to be a non-local path that points to some reliable storage
  //相关联的RDD已经被检查点的目录被假定为指向一些可靠存储的非本地路径
  private val cpDir: String =
    ReliableRDDCheckpointData.checkpointPath(rdd.context, rdd.id)
      .map(_.toString)
      //如果 Option 里有东西就拿出来,不然就给个默认值,抛出异常
      .getOrElse { throw new SparkException("Checkpoint dir must be specified.") }

  /**  
   * Return the directory to which this RDD was checkpointed.
    * 返回此RDD被检查点的目录,如果RDD尚未检查点,则返回None。
   * If the RDD is not checkpointed yet, return None.
   * 返回RDD检查点目录,
   */
  def getCheckpointDir: Option[String] = RDDCheckpointData.synchronized {
    if (isCheckpointed) {
      Some(cpDir.toString)
    } else {
      None
    }
  }

  /**
   * 实现RDD内容写一个可靠的DFS(分布式文件系统),检查点的数据写入
   * Materialize this RDD and write its content to a reliable DFS.
   * This is called immediately after the first action invoked on this RDD has completed.
    * 实现此RDD并将其内容写入可靠的DFS,在此RDD调用的第一个操作完成后立即调用。
   */
  protected override def doCheckpoint(): CheckpointRDD[T] = {
    // Create the output path for the checkpoint
    //创建一个保存checkpoint数据的目录
    val path = new Path(cpDir)
    val fs = path.getFileSystem(rdd.context.hadoopConfiguration)
    if (!fs.mkdirs(path)) {//
      throw new SparkException(s"Failed to create checkpoint path $cpDir")
    }
    //保存广播判变量,从重新加载RDD
    // Save to file, and reload it as an RDD
    val broadcastedConf = rdd.context.broadcast(
      new SerializableConfiguration(rdd.context.hadoopConfiguration))
    // TODO: This is expensive because it computes the RDD again unnecessarily (SPARK-8582)
    //开始一个新的Job进行计算,计算结果存入路径path中
    rdd.context.runJob(rdd, ReliableCheckpointRDD.writeCheckpointFile[T](cpDir, broadcastedConf) _)
    //根据结果路径path来创建ReliableCheckpointRDD
    val newRDD = new ReliableCheckpointRDD[T](rdd.context, cpDir)
    if (newRDD.partitions.length != rdd.partitions.length) {
      throw new SparkException(
        s"Checkpoint RDD $newRDD(${newRDD.partitions.length}) has different " +
          s"number of partitions from original RDD $rdd(${rdd.partitions.length})")
    }

    // Optionally clean our checkpoint files if the reference is out of scope
    //保存结果,清除原始RDD的依赖,Partition信息
    if (rdd.conf.getBoolean("spark.cleaner.referenceTracking.cleanCheckpoints", false)) {
      rdd.context.cleaner.foreach { cleaner =>
        cleaner.registerRDDCheckpointDataForCleanup(newRDD, rdd.id)
      }
    }

    logInfo(s"Done checkpointing RDD ${rdd.id} to $cpDir, new parent is RDD ${newRDD.id}")

    newRDD
  }

}

private[spark] object ReliableRDDCheckpointData {

  /** 
   *  Return the path of the directory to which this RDD's checkpoint data is written. 
   *  创建一个保存checkpoint数据的目录
   *  */
  def checkpointPath(sc: SparkContext, rddId: Int): Option[Path] = {
    sc.checkpointDir.map { dir => new Path(dir, s"rdd-$rddId") }
  }

  /** 
   *  Clean up the files associated with the checkpoint data for this RDD.   
   *  删除RDD检查点数据相关的文件
   *  */
  def cleanCheckpoint(sc: SparkContext, rddId: Int): Unit = {
    checkpointPath(sc, rddId).foreach { path =>
      val fs = path.getFileSystem(sc.hadoopConfiguration)
      if (fs.exists(path)) {
        fs.delete(path, true)
      }
    }
  }
}
