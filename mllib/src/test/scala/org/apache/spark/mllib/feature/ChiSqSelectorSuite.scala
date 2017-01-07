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

package org.apache.spark.mllib.feature

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLlibTestSparkContext
/**
 * 特征提取和转换 卡方选择(ChiSqSelector)
 * ChiSqSelector代表卡方特征选择,它适用于带有类别特征的标签数据。
 * ChiSqSelector根据类别的独立卡方2检验来对特征排序,然后选取类别标签主要依赖的特征,
 * 它类似于选取最有预测能力的特征
 */
class ChiSqSelectorSuite extends SparkFunSuite with MLlibTestSparkContext {

  /*
   *  Contingency tables 列联表数
   *  
   *  feature0 = {8.0, 0.0} 特征0
   *  class  0 1 2
   *    8.0||1|0|1|
   *    0.0||0|2|0|
   *
   *  feature1 = {7.0, 9.0} 特征1
   *  class  0 1 2
   *    7.0||1|0|0|
   *    9.0||0|2|1|
   *
   *  feature2 = {0.0, 6.0, 8.0, 5.0} 特征2
   *  class  0 1 2
   *    0.0||1|0|0|
   *    6.0||0|1|0|
   *    8.0||0|1|0|
   *    5.0||0|0|1|
   *
   *  Use chi-squared calculator from Internet
   *  从互联网上使用卡方计算器
   */
  //特征提取和转换 卡方选择(ChiSqSelector)稀疏和稠密向量
  test("ChiSqSelector transform test (sparse & dense vector)") {
    val labeledDiscreteData = sc.parallelize(//标记的离散数据
      Seq(LabeledPoint(0.0, Vectors.sparse(3, Array((0, 8.0), (1, 7.0)))),
        LabeledPoint(1.0, Vectors.sparse(3, Array((1, 9.0), (2, 6.0)))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0, 9.0, 8.0))),
        LabeledPoint(2.0, Vectors.dense(Array(8.0, 9.0, 5.0)))), 2)
    val preFilteredData =//预过滤数据
      Set(LabeledPoint(0.0, Vectors.dense(Array(0.0))),
        LabeledPoint(1.0, Vectors.dense(Array(6.0))),
        LabeledPoint(1.0, Vectors.dense(Array(8.0))),
        LabeledPoint(2.0, Vectors.dense(Array(5.0))))
    val model = new ChiSqSelector(1).fit(labeledDiscreteData)
    val filteredData = labeledDiscreteData.map { lp =>
      LabeledPoint(lp.label, model.transform(lp.features))
    }.collect().toSet
    assert(filteredData == preFilteredData)
  }
}
