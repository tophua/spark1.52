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

import java.io.File
import java.io.FilenameFilter
import java.io.IOException
import java.io.PrintWriter
import java.util.StringTokenizer

import scala.collection.JavaConversions._
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.reflect.ClassTag

import org.apache.spark.{Partition, SparkEnv, TaskContext}
import org.apache.spark.util.Utils


/**
 * An RDD that pipes the contents of each parent partition through an external command
 * (printing them one per line) and returns the output as a collection of strings.
  * 通过外部命令管理每个父分区的内容的RDD并返回输出作为字符串的集合
 */
private[spark] class PipedRDD[T: ClassTag](
    prev: RDD[T],
    command: Seq[String],
    envVars: Map[String, String],
    printPipeContext: (String => Unit) => Unit,
    printRDDElement: (T, String => Unit) => Unit,
    separateWorkingDir: Boolean)
  extends RDD[String](prev) {

  // Similar to Runtime.exec(), if we are given a single string, split it into words
  // using a standard StringTokenizer (i.e. by spaces)
  def this(
      prev: RDD[T],
      command: String,
      envVars: Map[String, String] = Map(),
      printPipeContext: (String => Unit) => Unit = null,
      printRDDElement: (T, String => Unit) => Unit = null,
      separateWorkingDir: Boolean = false) =
    this(prev, PipedRDD.tokenize(command), envVars, printPipeContext, printRDDElement,
      separateWorkingDir)

  //getPartitions返回的就是第一个父类的partitions
  override def getPartitions: Array[Partition] = firstParent[T].partitions

  /**
   * A FilenameFilter that accepts anything that isn't equal to the name passed in.
    * 一个FilenameFilter,它接受与传入的名称不相等的任何东西
   * @param filterName of file or directory to leave out
   */
  class NotEqualsFileNameFilter(filterName: String) extends FilenameFilter {
    def accept(dir: File, name: String): Boolean = {
      !name.equals(filterName)
    }
  }
//调用了第一个父类的compute,把结果RDD copy返回
  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    val pb = new ProcessBuilder(command)
    // Add the environmental variables to the process.
    val currentEnvVars = pb.environment()
    envVars.foreach { case (variable, value) => currentEnvVars.put(variable, value) }

    // for compatibility with Hadoop which sets these env variables
    // so the user code can access the input filename
    if (split.isInstanceOf[HadoopPartition]) {
      val hadoopSplit = split.asInstanceOf[HadoopPartition]
      currentEnvVars.putAll(hadoopSplit.getPipeEnvVars())
    }

    // When spark.worker.separated.working.directory option is turned on, each
    // task will be run in separate directory. This should be resolve file
    // access conflict issue 每个任务将在单独的目录中运行,这应该是解决文件访问冲突的问题
    val taskDirectory = "tasks" + File.separator + java.util.UUID.randomUUID.toString
    var workInTaskDirectory = false
    logDebug("taskDirectory = " + taskDirectory)
    if (separateWorkingDir) {
      val currentDir = new File(".")
      logDebug("currentDir = " + currentDir.getAbsolutePath())
      val taskDirFile = new File(taskDirectory)
      taskDirFile.mkdirs()

      try {
        val tasksDirFilter = new NotEqualsFileNameFilter("tasks")

        // Need to add symlinks to jars, files, and directories.  On Yarn we could have
        // directories and other files not known to the SparkContext that were added via the
        // Hadoop distributed cache.  We also don't want to symlink to the /tasks directories we
        // are creating here.
        //需要添加链接到文件和目录的jar,我们可以对纱线的目录和其他文件,不知道的sparkcontext被添加通过Hadoop分布式缓存
        //我们也不想链接我们正在/任务目录
        for (file <- currentDir.list(tasksDirFilter)) {
          val fileWithDir = new File(currentDir, file)
          Utils.symlink(new File(fileWithDir.getAbsolutePath()),
            new File(taskDirectory + File.separator + fileWithDir.getName()))
        }
        pb.directory(taskDirFile)
        workInTaskDirectory = true
      } catch {
        case e: Exception => logError("Unable to setup task working directory: " + e.getMessage +
          " (" + taskDirectory + ")", e)
      }
    }

    val proc = pb.start()
    val env = SparkEnv.get

    // Start a thread to print the process's stderr to ours
    //开始一个线程打印过程的stderr
    new Thread("stderr reader for " + command) {
      override def run() {
        for (line <- Source.fromInputStream(proc.getErrorStream).getLines) {
          // scalastyle:off println
          System.err.println(line)
          // scalastyle:on println
        }
      }
    }.start()

    // Start a thread to feed the process input from our parent's iterator
    //启动一个线程来从父代的迭代器提供进程输入
    new Thread("stdin writer for " + command) {
      override def run() {
        TaskContext.setTaskContext(context)
        val out = new PrintWriter(proc.getOutputStream)

        // scalastyle:off println
        // input the pipe context firstly
        //首先输入管道上下文
        if (printPipeContext != null) {
          printPipeContext(out.println(_))
        }
        for (elem <- firstParent[T].iterator(split, context)) {
          if (printRDDElement != null) {
            printRDDElement(elem, out.println(_))
          } else {
            out.println(elem)
          }
        }
        // scalastyle:on println
        out.close()
      }
    }.start()

    // Return an iterator that read lines from the process's stdout
    //返回一个从进程的标准输出读取行的迭代器
    val lines = Source.fromInputStream(proc.getInputStream).getLines()
    new Iterator[String] {
      def next(): String = lines.next()
      def hasNext: Boolean = {
        if (lines.hasNext) {
          true
        } else {
          val exitStatus = proc.waitFor()
          if (exitStatus != 0) {
            throw new Exception("Subprocess exited with status " + exitStatus)
          }

          // cleanup task working directory if used
          //清理任务工作目录(如果使用)
          if (workInTaskDirectory) {
            scala.util.control.Exception.ignoring(classOf[IOException]) {
              Utils.deleteRecursively(new File(taskDirectory))
            }
            logDebug("Removed task working directory " + taskDirectory)
          }

          false
        }
      }
    }
  }
}

private object PipedRDD {
  // Split a string into words using a standard StringTokenizer
  //使用标准StringTokenizer将字符串拆分为单词
  def tokenize(command: String): Seq[String] = {
    val buf = new ArrayBuffer[String]
    val tok = new StringTokenizer(command)
    while(tok.hasMoreElements) {
      buf += tok.nextToken()
    }
    buf
  }
}
