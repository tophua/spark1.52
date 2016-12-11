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

package org.apache.spark.streaming.dstream

import scala.collection.mutable.HashMap
import scala.reflect.ClassTag
import java.io.{ObjectOutputStream, ObjectInputStream, IOException}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.Logging
import org.apache.spark.streaming.Time
import org.apache.spark.util.Utils

private[streaming]
class DStreamCheckpointData[T: ClassTag] (dstream: DStream[T])
  extends Serializable with Logging {
  protected val data = new HashMap[Time, AnyRef]()

  // Mapping of the batch time to the checkpointed RDD file of that time
  //该批处理时间的检查点文件的时间映射
  @transient private var timeToCheckpointFile = new HashMap[Time, String]
  // Mapping of the batch time to the time of the oldest checkpointed RDD
  // in that batch's checkpoint data
  //该批的时候,批的检查点数据建立时间映射旧的RDD
  @transient private var timeToOldestCheckpointFileTime = new HashMap[Time, Time]

  @transient private var fileSystem : FileSystem = null
  protected[streaming] def currentCheckpointFiles = data.asInstanceOf[HashMap[Time, String]]

  /**
   * Updates the checkpoint data of the DStream. This gets called every time
   * the graph checkpoint is initiated. Default implementation records the
   * 更新的dstream检查点数据,每次启动图形检查点时都会被调用
   * checkpoint files to which the generate RDDs of the DStream has been saved.
   * 默认实现记录的检查点文件,生成的dstream RDDS保存
   */
  def update(time: Time) {

    // Get the checkpointed RDDs from the generated RDDs
    val checkpointFiles = dstream.generatedRDDs.filter(_._2.getCheckpointFile.isDefined)
                                       .map(x => (x._1, x._2.getCheckpointFile.get))
    logDebug("Current checkpoint files:\n" + checkpointFiles.toSeq.mkString("\n"))

    // Add the checkpoint files to the data to be serialized
    //添加检查点文件将数据序列化
    if (!checkpointFiles.isEmpty) {
      currentCheckpointFiles.clear()
      currentCheckpointFiles ++= checkpointFiles
      // Add the current checkpoint files to the map of all checkpoint files
      // This will be used to delete old checkpoint files
      timeToCheckpointFile ++= currentCheckpointFiles
      // Remember the time of the oldest checkpoint RDD in current state
      timeToOldestCheckpointFileTime(time) = currentCheckpointFiles.keys.min(Time.ordering)
    }
  }

  /**
   * Cleanup old checkpoint data. This gets called after a checkpoint of `time` has been
   * 清除旧的检查点数据,这被调用后的一个检查点已被写入到检查点目录
   * written to the checkpoint directory.
   */
  def cleanup(time: Time) {
    // Get the time of the oldest checkpointed RDD that was written as part of the
    // checkpoint of `time`
    //有时间最早的检查点，写的是RDD的一部分`时间`检查站
    timeToOldestCheckpointFileTime.remove(time) match {
      case Some(lastCheckpointFileTime) =>
        // Find all the checkpointed RDDs (i.e. files) that are older than `lastCheckpointFileTime`
        // This is because checkpointed RDDs older than this are not going to be needed
        // even after master fails, as the checkpoint data of `time` does not refer to those files
        val filesToDelete = timeToCheckpointFile.filter(_._1 < lastCheckpointFileTime)
        logDebug("Files to delete:\n" + filesToDelete.mkString(","))
        filesToDelete.foreach {
          case (time, file) =>
            try {
              val path = new Path(file)
              if (fileSystem == null) {
                fileSystem = path.getFileSystem(dstream.ssc.sparkContext.hadoopConfiguration)
              }
              fileSystem.delete(path, true)
              timeToCheckpointFile -= time
              logInfo("Deleted checkpoint file '" + file + "' for time " + time)
            } catch {
              case e: Exception =>
                logWarning("Error deleting old checkpoint file '" + file + "' for time " + time, e)
                fileSystem = null
            }
        }
      case None =>
        logDebug("Nothing to delete")
    }
  }

  /**
   * Restore the checkpoint data. This gets called once when the DStream graph
   * 恢复检查点数据,这被称为一次dstream图从图检查点文件恢复
   * (along with its DStreams) are being restored from a graph checkpoint file.
   * Default implementation restores the RDDs from their checkpoint files.
   * 默认实现恢复RDDS从检查点文件
   */
  def restore() {
    // Create RDDs from the checkpoint data
    //从检查点数据创建RDDS
    currentCheckpointFiles.foreach {
      case(time, file) => {
        logInfo("Restoring checkpointed RDD for time " + time + " from file '" + file + "'")
        //HashMap 相加
        dstream.generatedRDDs += ((time, dstream.context.sparkContext.checkpointFile[T](file)))
      }
    }
  }

  override def toString: String = {
    "[\n" + currentCheckpointFiles.size + " checkpoint files \n" +
      currentCheckpointFiles.mkString("\n") + "\n]"
  }

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    logDebug(this.getClass().getSimpleName + ".writeObject used")
    if (dstream.context.graph != null) {
      dstream.context.graph.synchronized {
        if (dstream.context.graph.checkpointInProgress) {
          oos.defaultWriteObject()
        } else {
          val msg = "Object of " + this.getClass.getName + " is being serialized " +
            " possibly as a part of closure of an RDD operation. This is because " +
            " the DStream object is being referred to from within the closure. " +
            " Please rewrite the RDD operation inside this DStream to avoid this. " +
            " This has been enforced to avoid bloating of Spark tasks " +
            " with unnecessary objects."
          throw new java.io.NotSerializableException(msg)
        }
      }
    } else {
      throw new java.io.NotSerializableException(
        "Graph is unexpectedly null when DStream is being serialized.")
    }
  }

  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream): Unit = Utils.tryOrIOException {
    logDebug(this.getClass().getSimpleName + ".readObject used")
    ois.defaultReadObject()
    timeToOldestCheckpointFileTime = new HashMap[Time, Time]
    timeToCheckpointFile = new HashMap[Time, String]
  }
}
