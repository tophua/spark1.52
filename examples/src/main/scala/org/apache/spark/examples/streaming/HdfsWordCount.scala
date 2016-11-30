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

// scalastyle:off println
package org.apache.spark.examples.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Counts words in new text files created in the given directory
 * 在给定目录中创建的新文本文件中的单词数
 * Usage: HdfsWordCount <directory>
 *   <directory> is the directory that Spark Streaming will use to find and read new text files.
 *	是Spark流将用于查找和读取新的文本文件的目录
 * To run this on your local machine on directory `localdir`, run this example
 * 运行在本地机器上的目录'localdir',运行此示例
 *    $ bin/run-example \
 *       org.apache.spark.examples.streaming.HdfsWordCount localdir
 *
 * Then create a text file in `localdir` and the words in the file will get counted.
 * 然后创建一个文本文件中的` localdir `和文件的话会计算
 */
object HdfsWordCount {
  def main(args: Array[String]) {
 /*   if (args.length < 1) {
      System.err.println("Usage: HdfsWordCount <directory>")
      System.exit(1)
    }*/

    StreamingExamples.setStreamingLogLevels()
    
    val mast="spark://dept3:8088"
    val hdfs="hdfs://xcsq:8089/cookbook/input/"
     //创建SparkConf对象
    val sparkConf = new SparkConf().setAppName("HdfsWordCount").setMaster(mast)
    // Create the context创建上下文
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    //创建目录的fileinputdstream和使用流计数单词创建新文件
    //val lines = ssc.textFileStream(args(0))
     //如果目录中有新创建的文件,则读取
    val lines = ssc.textFileStream(hdfs)
   //分割为单词
    val words = lines.flatMap(_.split(" ")) 
    //统计单词出现次数
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    //打印结果
    wordCounts.print()
    //启动Spark Streaming
    ssc.start()
    //一直运行,除非人为干预再停止
    ssc.awaitTermination()
  }
}
// scalastyle:on println
