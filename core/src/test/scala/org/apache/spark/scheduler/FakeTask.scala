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

package org.apache.spark.scheduler

import org.apache.spark.TaskContext

class FakeTask(
    stageId: Int,
    prefLocs: Seq[TaskLocation] = Nil)
  extends Task[Int](stageId, 0, 0, Seq.empty) {//扩展一个Task类
  override def runTask(context: TaskContext): Int = 0
  override def preferredLocations: Seq[TaskLocation] = prefLocs
}

object FakeTask {//假任务
  /**
   * Utility method to create a TaskSet, potentially setting a particular sequence of preferred  
   * locations for each task (given as varargs) if this sequence is not empty.
   * 实用的方法来创建一个taskset,可能设置为每个任务的优先位置特定的顺序,如果这个序列不是空
   */
  def createTaskSet(numTasks: Int, prefLocs: Seq[TaskLocation]*): TaskSet = {
    createTaskSet(numTasks, 0, prefLocs: _*)
  }

  def createTaskSet(numTasks: Int, stageAttemptId: Int, prefLocs: Seq[TaskLocation]*): TaskSet = {
    if (prefLocs.size != 0 && prefLocs.size != numTasks) {
      //错误的任务位置
      throw new IllegalArgumentException("Wrong number of task locations")
    }
    //tabulate返回指定长度数组,每个数组元素为指定函数的返回值,默认从 0开始
    /**
     * 实例返回 3 个元素：
     * scala> Array.tabulate(3)(a => a + 5)
		 * res0: Array[Int] = Array(5, 6, 7)
     */
    val tasks = Array.tabulate[Task[_]](numTasks) { i =>
      new FakeTask(i, if (prefLocs.size != 0) prefLocs(i) else Nil)
    }
    new TaskSet(tasks, 0, stageAttemptId, 0, null)
  }
}
