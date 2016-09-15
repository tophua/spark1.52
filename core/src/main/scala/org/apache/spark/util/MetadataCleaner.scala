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

package org.apache.spark.util

import java.util.{Timer, TimerTask}

import org.apache.spark.{Logging, SparkConf}

/**
 * Runs a timer task to periodically clean up metadata (e.g. old files or hashtable entries)
 * MetadataCleaner运行定时任务周期性的清理元数据
 */
private[spark] class MetadataCleaner(
    cleanerType: MetadataCleanerType.MetadataCleanerType,//清理的元数据类型
    cleanupFunc: (Long) => Unit,//清理函数，MetadataCleaner以periodSeconds为间隔周期性的调用该函数
    conf: SparkConf)
  extends Logging
{
  
  val name = cleanerType.toString//清理的元数据类型
 //表示数据多少秒过期
  private val delaySeconds = MetadataCleaner.getDelaySeconds(conf, cleanerType)
  //清理周期,即periodSeconds的间隔周期性调用清理函数来判断是否过期 
  private val periodSeconds = math.max(10, delaySeconds / 10)
  //定时调度器
  private val timer = new Timer(name + " cleanup timer", true)
  //清理任务,定时器实现,不断调用cleanupFunc函数,
  private val task = new TimerTask {
    override def run() {
      try {
        //该函数实现逻辑是判断数据存储的时间戳是否小于传入的参数,若小于则表示过期,需要清理
        //否则没有过期
        cleanupFunc(System.currentTimeMillis() - (delaySeconds * 1000))//清理persistentRdds中的过期内容
        logInfo("Ran metadata cleaner for " + name)
      } catch {
        case e: Exception => logError("Error running cleanup task for " + name, e)
      }
    }
  }

  if (delaySeconds > 0) {
    logDebug(
      "Starting metadata cleaner for " + name + " with delay of " + delaySeconds + " seconds " +
      "and period of " + periodSeconds + " secs")
      //delaySeconds 延迟毫秒数,periodSeconds间隔周期调用清理毫秒,task调用执行任务
    timer.schedule(task, delaySeconds * 1000, periodSeconds * 1000)
  }

  def cancel() {
    timer.cancel()
  }
}

private[spark] object MetadataCleanerType extends Enumeration {
  //枚举类
  /**
   * MAP_OUTPUT_TRACKER，executor跟踪各个map任务输出的存储位置的数据，根据spark.cleaner.ttl.MAP_OUTPUT_TRACKER设置清理时间，默认值为-1，表示不清理
   * SPARK_CONTEXT，SparkContext中记录缓存到内存中的RDD的数据结构，根据spark.cleaner.ttl.SPARK_CONTEXT设置清理时间，默认值为-1，表示不清理
   * HTTP_BROADCAST，采用http方式广播broadcast的元数据，根据spark.cleaner.ttl.HTTP_BROADCAST设置清理时间，默认值为-1，表示不清理
   * BLOCK_MANAGER，BlockManager中非Broadcast类型的Block数据，根据spark.cleaner.ttl.BLOCK_MANAGER设置清理时间，默认值为-1，表示不清理；
   * SHUFFLE_BLOCK_MANAGER，shuffle输出的数据，根据spark.cleaner.ttl.SHUFFLE_BLOCK_MANAGER设置清理时间，默认值为-1，表示不清理；
   * BROADCAST_VARS，Torrent方式广播broadcast的元数据，底层依赖于BlockManager，根据spark.cleaner.ttl.BROADCAST_VARS设置清理时间，默认值为-1，表示不清理
   */
  val MAP_OUTPUT_TRACKER, SPARK_CONTEXT, HTTP_BROADCAST, BLOCK_MANAGER,
  SHUFFLE_BLOCK_MANAGER, BROADCAST_VARS = Value
 //枚举别名
  type MetadataCleanerType = Value

  def systemProperty(which: MetadataCleanerType.MetadataCleanerType): String = {
    "spark.cleaner.ttl." + which.toString
  }
}

// TODO: This mutates a Conf to set properties right now, which is kind of ugly when used in the
// initialization of StreamingContext. It's okay for users trying to configure stuff themselves.
private[spark] object MetadataCleaner {
  def getDelaySeconds(conf: SparkConf): Int = {
    //spark记录任何元数据的持续时间,设置清理时间  -1清理
    //周期性清除保证在这个时间之前的元数据会被清理。
    //当程序长时间几小时，几天的运行Spark的时候设置这个是很有用的。注意：任何内存中的RDD只要过了这个时间就会被清除掉。
    conf.getTimeAsSeconds("spark.cleaner.ttl", "-1").toInt
  }

  def getDelaySeconds(
      conf: SparkConf,
      cleanerType: MetadataCleanerType.MetadataCleanerType): Int = {
    conf.get(MetadataCleanerType.systemProperty(cleanerType), getDelaySeconds(conf).toString).toInt
  }

  def setDelaySeconds(
      conf: SparkConf,
      cleanerType: MetadataCleanerType.MetadataCleanerType,
      delay: Int) {
    conf.set(MetadataCleanerType.systemProperty(cleanerType), delay.toString)
  }

  /**
   * Set the default delay time (in seconds).
   * 设置默认的延迟时间(在几秒钟内)
   * @param conf SparkConf instance
   * @param delay default delay time to set,延迟默认延迟时间
   * @param resetAll whether to reset all to default 是否重置
   */
  def setDelaySeconds(conf: SparkConf, delay: Int, resetAll: Boolean = true) {
    //Spark记忆任何元数据(stages生成，任务生成等等)的时间(秒)。周期性清除保证在这个时间之前的元数据会被遗忘。
    //当长时间几小时，几天的运行Spark的时候设置这个是很有用的。注意：任何内存中的RDD只要过了这个时间就会被清除掉。
    conf.set("spark.cleaner.ttl", delay.toString)
    if (resetAll) {
      for (cleanerType <- MetadataCleanerType.values) {
        System.clearProperty(MetadataCleanerType.systemProperty(cleanerType))
      }
    }
  }
}

