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

package org.apache.spark.ml.regression

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.impl.TreeTests
import org.apache.spark.ml.util.MLTestingUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{DecisionTree => OldDecisionTree,
  DecisionTreeSuite => OldDecisionTreeSuite}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
 * 决策树以及其集成算法是机器学习分类和回归问题中非常流行的算法
 * 工具提供为分类提供两种不纯度衡量(基尼不纯度和熵),为回归提供一种不纯度衡量(方差)
 */

class DecisionTreeRegressorSuite extends SparkFunSuite with MLlibTestSparkContext {

  import DecisionTreeRegressorSuite.compareAPIs

  private var categoricalDataPointsRDD: RDD[LabeledPoint] = _

  override def beforeAll() {
    super.beforeAll()
    categoricalDataPointsRDD =
      sc.parallelize(OldDecisionTreeSuite.generateCategoricalDataPoints())
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests calling train()
  /////////////////////////////////////////////////////////////////////////////
  //三元回归树(有序)的类别特征
  test("Regression stump with 3-ary (ordered) categorical features") {
    val dt = new DecisionTreeRegressor()
      .setImpurity("variance")//设置纯度,方差
      .setMaxDepth(2)//最大深度
      .setMaxBins(100)//最大桶数
    val categoricalFeatures = Map(0 -> 3, 1-> 3)
    compareAPIs(categoricalDataPointsRDD, dt, categoricalFeatures)
  }
  //具有二元(有序)分类特征的回归分析
  test("Regression stump with binary (ordered) categorical features") {
    val dt = new DecisionTreeRegressor()
      .setImpurity("variance")//计算信息增益的准则
      .setMaxDepth(2)//树的最大深度
      .setMaxBins(100)//连续特征离散化的最大数量，以及选择每个节点分裂特征的方式
    val categoricalFeatures = Map(0 -> 2, 1-> 2)
    compareAPIs(categoricalDataPointsRDD, dt, categoricalFeatures)
  }

  test("copied model must have the same parent") {//复制的模型必须有相同的父
    val categoricalFeatures = Map(0 -> 2, 1-> 2)
    val df = TreeTests.setMetadata(categoricalDataPointsRDD, categoricalFeatures, numClasses = 0)
    val model = new DecisionTreeRegressor()
      .setImpurity("variance")//计算信息增益的准则
      .setMaxDepth(2)//树的最大深度
      .setMaxBins(8).fit(df)//连续特征离散化的最大数量，以及选择每个节点分裂特征的方式
    MLTestingUtils.checkCopy(model)
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests of model save/load 测试模型保存/加载
  /////////////////////////////////////////////////////////////////////////////

  // TODO: test("model save/load")   SPARK-6725
}

private[ml] object DecisionTreeRegressorSuite extends SparkFunSuite {

  /**
   * Train 2 decision trees on the given dataset, one using the old API and one using the new API.
   * 在给定的数据集上训练2个决策树,一个使用旧的和一个使用新的应用
   * Convert the old tree to the new format, compare them, and fail if they are not exactly equal.
   * 将旧的树转换为新的格式,比较他们,如果他们不完全平等的话,就失败了
   */
  def compareAPIs(
      data: RDD[LabeledPoint],
      dt: DecisionTreeRegressor,
      categoricalFeatures: Map[Int, Int]): Unit = {
    val oldStrategy = dt.getOldStrategy(categoricalFeatures)
    val oldTree = OldDecisionTree.train(data, oldStrategy)
    val newData: DataFrame = TreeTests.setMetadata(data, categoricalFeatures, numClasses = 0)
    val newTree = dt.fit(newData)
    // Use parent from newTree since this is not checked anyways.
    val oldTreeAsNew = DecisionTreeRegressionModel.fromOld(
      oldTree, newTree.parent.asInstanceOf[DecisionTreeRegressor], categoricalFeatures)
    TreeTests.checkEqual(oldTreeAsNew, newTree)
  }
}
