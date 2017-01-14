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

package org.apache.spark.mllib.evaluation

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext
/**
 * RankingMetrics类用来计算基于排名的评估指标,K值平均准确率函数
 * 输出指定K值时的平均准确度,APK它用于衡量针对某个查询返回的"前K个"文档的平均相关性
 * 如果结果中文档的实际相关性越高且排名也更靠前,那APK分值也就越高,适合评估的好坏,因为推荐系统
 * 也会计算前K个推荐物,然后呈现给用户,APK和其他基于排名的指标同样适合评估隐式数据集上的推荐
 * 这里用MSE相对就不那么适合,APK平均准确率
 */
class RankingMetricsSuite extends SparkFunSuite with MLlibTestSparkContext {
  test("Ranking metrics: map, ndcg") {
    val predictionAndLabels = sc.parallelize(
      Seq(
          //一个键值对类型的RDD,其键为给定用户预测的物品的ID数组,而值则是实际的物品ID数组
        (Array[Int](1, 6, 2, 7, 8, 3, 9, 10, 4, 5), Array[Int](1, 2, 3, 4, 5)),
        (Array[Int](4, 1, 5, 6, 2, 7, 3, 8, 9, 10), Array[Int](1, 2, 3)),
        (Array[Int](1, 2, 3, 4, 5), Array[Int]())
      ), 2)
    val eps: Double = 1E-5
   //需要向我们之前的平均准确率函数传入一个键值对类型的RDD,
   //其键为给定用户预测的物品的ID数组,而值则是实际的物品ID数组
    val metrics = new RankingMetrics(predictionAndLabels)
    //平均准确率(平均正确率值)
    val map = metrics.meanAveragePrecision
   //精度位置
    println("precisionAt:"+metrics.precisionAt(1)+" \t"+1.0/3)
    //计算所有的查询的平均准确率,截断在排名位置    

    assert(metrics.precisionAt(1) ~== 1.0/3 absTol eps)
    assert(metrics.precisionAt(2) ~== 1.0/3 absTol eps)
    assert(metrics.precisionAt(3) ~== 1.0/3 absTol eps)
    assert(metrics.precisionAt(4) ~== 0.75/3 absTol eps)
    assert(metrics.precisionAt(5) ~== 0.8/3 absTol eps)
    assert(metrics.precisionAt(10) ~== 0.8/3 absTol eps)
    assert(metrics.precisionAt(15) ~== 8.0/45 absTol eps)

    assert(map ~== 0.355026 absTol eps)
     /**
     * 排名指标是MAP和NDCG
     * NDCG表示归一化折损累积增益
     * 平均精度均值（MAP）
     * 
     * 两者之间的主要区别是，
     * MAP认为是二元相关性（一个项是感兴趣的或者不感兴趣的）
     * 而NDCG允许以实数形式进行相关性打分
     * 
     * 一个推荐系统返回一些项并形成一个列表，我们想要计算这个列表有多好。
     * 每一项都有一个相关的评分值，通常这些评分值是一个非负数。这就是gain（增益）
     * 
     */
    //ndcg 归一化折损累积增益
    assert(metrics.ndcgAt(3) ~== 1.0/3 absTol eps)
    assert(metrics.ndcgAt(5) ~== 0.328788 absTol eps)
    assert(metrics.ndcgAt(10) ~== 0.487913 absTol eps)
    assert(metrics.ndcgAt(15) ~== metrics.ndcgAt(10) absTol eps)

  }
}
