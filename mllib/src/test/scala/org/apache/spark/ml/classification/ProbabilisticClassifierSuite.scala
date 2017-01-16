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

package org.apache.spark.ml.classification

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.{Vector, Vectors}

final class TestProbabilisticClassificationModel(
    override val uid: String,
    override val numClasses: Int)
  extends ProbabilisticClassificationModel[Vector, TestProbabilisticClassificationModel] {

  override def copy(extra: org.apache.spark.ml.param.ParamMap): this.type = defaultCopy(extra)

  override protected def predictRaw(input: Vector): Vector = {
    input
  }

  override protected def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
    rawPrediction
  }

  def friendlyPredict(input: Vector): Double = {
    predict(input)
  }
}

//概率分类器套件
class ProbabilisticClassifierSuite extends SparkFunSuite {

  test("test thresholding") {//测试阈值
    val thresholds = Array(0.5, 0.2)
     //在二进制分类中设置阈值,范围为[0，1],如果类标签1的估计概率>Threshold,则预测1,否则0
    val testModel = new TestProbabilisticClassificationModel("myuid", 2).setThresholds(thresholds)
    assert(testModel.friendlyPredict(Vectors.dense(Array(1.0, 1.0))) === 1.0)
    assert(testModel.friendlyPredict(Vectors.dense(Array(1.0, 0.2))) === 0.0)
  }

  test("test thresholding not required") {//测试不需要阈值
    val testModel = new TestProbabilisticClassificationModel("myuid", 2)
    assert(testModel.friendlyPredict(Vectors.dense(Array(1.0, 2.0))) === 1.0)
  }
}
