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
package org.apache.spark.examples.mllib

import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 聚类 流式K均值
 * Estimate clusters on one stream of data and make predictions
 * 在一个数据流上估计集群，并在另一个流上进行预测,
 * on another stream, where the data streams arrive as text files
 * into two different directories.
 * 当数据流到达文本文件到两个不同的目录时
 *
 * The rows of the training text files must be vector data in the form
 * 训练文本文件的行必须是窗体中的矢量数据,其中n是维数的数目
 * `[x1,x2,x3,...,xn]`
 * Where n is the number of dimensions.
 *
 * The rows of the test text files must be labeled data in the form
 * 测试文本文件的行必须在窗体中标记数据
 * `(y,[x1,x2,x3,...,xn])`
 * Where y is some identifier. n must be the same for train and test.
 * 在哪里Y是一些标识符,训练和测试必须是相同的
 *
 * Usage:
 *   StreamingKMeansExample <trainingDir> <testDir> <batchDuration> <numClusters> <numDimensions>
 *
 * To run on your local machine using the two directories `trainingDir` and `testDir`,
 * 使用两个目录在本地机器上运行`trainingDir` 和 `testDir`,
 * with updates every 5 seconds, 2 dimensions per data point, and 3 clusters, call:
 * 每5秒更新一次,每一个数据点的2个维度,和3个族
 *    $ bin/run-example mllib.StreamingKMeansExample trainingDir testDir 5 3 2
 *
 * As you add text files to `trainingDir` the clusters will continuously update.
 * Anytime you add text files to `testDir`, you'll see predicted labels using the current model.
 *
 */
object StreamingKMeansExample {

  def main(args: Array[String]) {
    if (args.length != 5) {
      System.err.println(
        "Usage: StreamingKMeansExample " +
          "<trainingDir> <testDir> <batchDuration> <numClusters> <numDimensions>")
      System.exit(1)
    }

    val conf = new SparkConf().setMaster("local").setAppName("StreamingKMeansExample")
    //批次数
    val ssc = new StreamingContext(conf, Seconds(args(2).toLong))
    //文件流,训练目录,解析向量
    val trainingData = ssc.textFileStream(args(0)).map(Vectors.parse)
    //测试目录
    val testData = ssc.textFileStream(args(1)).map(LabeledPoint.parse)

    val model = new StreamingKMeans()
      //聚类中心点
      .setK(args(3).toInt)
      .setDecayFactor(1.0)
      //随机中心数
      .setRandomCenters(args(4).toInt, 0.0)

    model.trainOn(trainingData)
    model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
// scalastyle:on println
