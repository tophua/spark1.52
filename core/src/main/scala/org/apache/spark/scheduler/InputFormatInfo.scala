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

import scala.collection.JavaConversions._
import scala.collection.immutable.Set
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.util.ReflectionUtils

import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.deploy.SparkHadoopUtil

/**
 * :: DeveloperApi ::
 * Parses and holds information about inputFormat (and files) specified as a parameter.
 * 解析和信息保存关于InputFormat（和文件）指定为一个参数
 */
@DeveloperApi
class InputFormatInfo(val configuration: Configuration, val inputFormatClazz: Class[_],
    val path: String) extends Logging {

  var mapreduceInputFormat: Boolean = false
  var mapredInputFormat: Boolean = false

  validate()

  override def toString: String = {
    "InputFormatInfo " + super.toString + " .. inputFormatClazz " + inputFormatClazz + ", " +
      "path : " + path
  }

  override def hashCode(): Int = {
    var hashCode = inputFormatClazz.hashCode
    hashCode = hashCode * 31 + path.hashCode
    hashCode
  }

  // Since we are not doing canonicalization of path, this can be wrong : like relative vs
  // absolute path .. which is fine, this is best case effort to remove duplicates - right ?
  //既然我们没有做道路标准化，这可能是错误的：像亲戚vs
  //绝对路径..这是很好的，这是最好的情况下努力删除重复 - 对吗？
  override def equals(other: Any): Boolean = other match {
    case that: InputFormatInfo => {
      // not checking config - that should be fine, right ?
      //不检查配置 - 应该是好的，对吧？
      this.inputFormatClazz == that.inputFormatClazz &&
        this.path == that.path
    }
    case _ => false
  }

  private def validate() {
    logDebug("validate InputFormatInfo : " + inputFormatClazz + ", path  " + path)

    try {
      if (classOf[org.apache.hadoop.mapreduce.InputFormat[_, _]].isAssignableFrom(
        inputFormatClazz)) {
        logDebug("inputformat is from mapreduce package")
        mapreduceInputFormat = true
      }
      else if (classOf[org.apache.hadoop.mapred.InputFormat[_, _]].isAssignableFrom(
        inputFormatClazz)) {
        logDebug("inputformat is from mapred package")
        mapredInputFormat = true
      }
      else {
        throw new IllegalArgumentException("Specified inputformat " + inputFormatClazz +
          " is NOT a supported input format ? does not implement either of the supported hadoop " +
            "api's")
      }
    }
    catch {
      case e: ClassNotFoundException => {
        throw new IllegalArgumentException("Specified inputformat " + inputFormatClazz +
          " cannot be found ?", e)
      }
    }
  }


  // This method does not expect failures, since validate has already passed ...
  //此方法不期望失败,因为验证已经通过
  private def prefLocsFromMapreduceInputFormat(): Set[SplitInfo] = {
    val conf = new JobConf(configuration)
    SparkHadoopUtil.get.addCredentials(conf)
    FileInputFormat.setInputPaths(conf, path)

    val instance: org.apache.hadoop.mapreduce.InputFormat[_, _] =
      ReflectionUtils.newInstance(inputFormatClazz.asInstanceOf[Class[_]], conf).asInstanceOf[
        org.apache.hadoop.mapreduce.InputFormat[_, _]]
    val job = new Job(conf)

    val retval = new ArrayBuffer[SplitInfo]()
    val list = instance.getSplits(job)
    for (split <- list) {
      retval ++= SplitInfo.toSplitInfo(inputFormatClazz, path, split)
    }

    retval.toSet
  }

  // This method does not expect failures, since validate has already passed ...
  //此方法不期望失败,因为验证已经通过
  private def prefLocsFromMapredInputFormat(): Set[SplitInfo] = {
    val jobConf = new JobConf(configuration)
    SparkHadoopUtil.get.addCredentials(jobConf)
    FileInputFormat.setInputPaths(jobConf, path)

    val instance: org.apache.hadoop.mapred.InputFormat[_, _] =
      ReflectionUtils.newInstance(inputFormatClazz.asInstanceOf[Class[_]], jobConf).asInstanceOf[
        org.apache.hadoop.mapred.InputFormat[_, _]]

    val retval = new ArrayBuffer[SplitInfo]()
    instance.getSplits(jobConf, jobConf.getNumMapTasks()).foreach(
        elem => retval ++= SplitInfo.toSplitInfo(inputFormatClazz, path, elem)
    )

    retval.toSet
   }

  private def findPreferredLocations(): Set[SplitInfo] = {
    logDebug("mapreduceInputFormat : " + mapreduceInputFormat + ", mapredInputFormat : " +
      mapredInputFormat + ", inputFormatClazz : " + inputFormatClazz)
    if (mapreduceInputFormat) {
      prefLocsFromMapreduceInputFormat()
    }
    else {
      assert(mapredInputFormat)
      prefLocsFromMapredInputFormat()
    }
  }
}




object InputFormatInfo {
  /**
    Computes the preferred locations based on input(s) and returned a location to block map.    
    Typical use of this method for allocation would follow some algo like this:
	     计算基于输入（S）的首选位置,并返回一个块映射的位置。典型方法的使用将遵循下列规则:
    a) For each host, count number of splits hosted on that host.,对于每个主机,计数分割每个主机上
    b) Decrement the currently allocated containers on that host.将当前主机上的已分配的容器减量
    c) Compute rack info for each host and update rack -> count map based on (b). 根据（b）计算每个主机和更新机架的机架信息 - >计数图。
    d) Allocate nodes based on (c) 基于（c）分配节点
    e) On the allocation result, ensure that we dont allocate "too many" jobs on a single node
       (even if data locality on that is very high) : this is to prevent fragility of job if a
       single (or small set of) hosts go down.

    go to (a) until required nodes are allocated.转到（a），直到分配所需的节点。

    If a node 'dies', follow same procedure.如果节点“死”，请遵循相同的过程。

    PS: I know the wording here is weird, hopefully it makes some sense !
  */
  def computePreferredLocations(formats: Seq[InputFormatInfo]): Map[String, Set[SplitInfo]] = {

    val nodeToSplit = new HashMap[String, HashSet[SplitInfo]]
    for (inputSplit <- formats) {
      val splits = inputSplit.findPreferredLocations()

      for (split <- splits){
        val location = split.hostLocation
        val set = nodeToSplit.getOrElseUpdate(location, new HashSet[SplitInfo])
        set += split
      }
    }

    nodeToSplit.mapValues(_.toSet).toMap
  }
}
