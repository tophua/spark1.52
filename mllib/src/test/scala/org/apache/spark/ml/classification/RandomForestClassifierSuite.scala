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
import org.apache.spark.ml.impl.TreeTests
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.tree.LeafNode
import org.apache.spark.ml.util.MLTestingUtils
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{EnsembleTestHelper, RandomForest => OldRandomForest}
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

/**
 * Test suite for [[RandomForestClassifier]].
 * 随机森林树分类
 */
class RandomForestClassifierSuite extends SparkFunSuite with MLlibTestSparkContext {

  import RandomForestClassifierSuite.compareAPIs

  private var orderedLabeledPoints50_1000: RDD[LabeledPoint] = _
  private var orderedLabeledPoints5_20: RDD[LabeledPoint] = _

  override def beforeAll() {
    super.beforeAll()
    orderedLabeledPoints50_1000 =
      sc.parallelize(EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 50, 1000))
    orderedLabeledPoints5_20 =
      sc.parallelize(EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 5, 20))
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests calling train()
  /////////////////////////////////////////////////////////////////////////////
  //具有连续特征的二元分类测试
  def binaryClassificationTestWithContinuousFeatures(rf: RandomForestClassifier) {
    val categoricalFeatures = Map.empty[Int, Int]
    val numClasses = 2//numClasses 分类数
    val newRF = rf
      .setImpurity("Gini")//计算信息增益的准则
      .setMaxDepth(2)
      .setNumTrees(1)
      .setFeatureSubsetStrategy("auto")//每次分裂候选特征数量
      .setSeed(123)
    compareAPIs(orderedLabeledPoints50_1000, newRF, categoricalFeatures, numClasses)
  }

  test("params") {//参数
    ParamsSuite.checkParams(new RandomForestClassifier)
    val model = new RandomForestClassificationModel("rfc",
      Array(new DecisionTreeClassificationModel("dtc", new LeafNode(0.0, 0.0, null), 2)), 2, 2)
    ParamsSuite.checkParams(model)
  }
  //具有连续特征的二元分类
  test("Binary classification with continuous features:" +
    " comparing DecisionTree vs. RandomForest(numTrees = 1)") {
    val rf = new RandomForestClassifier()
    binaryClassificationTestWithContinuousFeatures(rf)
  }
  //具有连续特征和节点标识缓存的二元分类
  test("Binary classification with continuous features and node Id cache:" +
    " comparing DecisionTree vs. RandomForest(numTrees = 1)") {
    val rf = new RandomForestClassifier()
      .setCacheNodeIds(true)
    binaryClassificationTestWithContinuousFeatures(rf)
  }
  //交替的分类和连续特征与多标签测试索引
  test("alternating categorical and continuous features with multiclass labels to test indexing") {
    val arr = Array(
      LabeledPoint(0.0, Vectors.dense(1.0, 0.0, 0.0, 3.0, 1.0)),
      LabeledPoint(1.0, Vectors.dense(0.0, 1.0, 1.0, 1.0, 2.0)),
      LabeledPoint(0.0, Vectors.dense(2.0, 0.0, 0.0, 6.0, 3.0)),
      LabeledPoint(2.0, Vectors.dense(0.0, 2.0, 1.0, 3.0, 2.0))
    )
    val rdd = sc.parallelize(arr)
    val categoricalFeatures = Map(0 -> 3, 2 -> 2, 4 -> 4)
    val numClasses = 3//numClasses 分类数

    val rf = new RandomForestClassifier()
      .setImpurity("Gini")//计算信息增益的准则
      .setMaxDepth(5)
      .setNumTrees(2)
      .setFeatureSubsetStrategy("sqrt")//每次分裂候选特征数量
      .setSeed(12345)
    compareAPIs(rdd, rf, categoricalFeatures, numClasses)
  }
  //采样率在随机森林树
  test("subsampling rate in RandomForest"){
    val rdd = orderedLabeledPoints5_20
    val categoricalFeatures = Map.empty[Int, Int]
    val numClasses = 2//numClasses 分类数

    val rf1 = new RandomForestClassifier()
      .setImpurity("Gini")//计算信息增益的准则
      .setMaxDepth(2)
      .setCacheNodeIds(true)
      .setNumTrees(3)//numClasses 分类数
      .setFeatureSubsetStrategy("auto")//每次分裂候选特征数量
      .setSeed(123)
    compareAPIs(rdd, rf1, categoricalFeatures, numClasses)
   //subsamplingRate学习一棵决策树使用的训练数据比例,范围[0,1]
    val rf2 = rf1.setSubsamplingRate(0.5)
    compareAPIs(rdd, rf2, categoricalFeatures, numClasses)
  }
  //原预测和预测概率
  test("predictRaw and predictProbability") {
    val rdd = orderedLabeledPoints5_20
    val rf = new RandomForestClassifier()
      .setImpurity("Gini")//计算信息增益的准则
      .setMaxDepth(3)
      .setNumTrees(3)//numClasses 分类数
      .setSeed(123)
    val categoricalFeatures = Map.empty[Int, Int]
    val numClasses = 2//numClasses 分类数

    val df: DataFrame = TreeTests.setMetadata(rdd, categoricalFeatures, numClasses)
    //fit()方法将DataFrame转化为一个Transformer的算法
    val model = rf.fit(df)

    // copied model must have the same parent.
    //复制的模型必须有相同的父
    MLTestingUtils.checkCopy(model)
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val predictions = model.transform(df)
       //原始的算法预测结果的存储列的名称, 默认是”rawPrediction”
       //类别预测结果的条件概率值存储列的名称, 默认值是”probability”
      .select(rf.getPredictionCol, rf.getRawPredictionCol, rf.getProbabilityCol)
      .collect()

    predictions.foreach { case Row(pred: Double, rawPred: Vector, probPred: Vector) =>
      assert(pred === rawPred.argmax,
        s"Expected prediction $pred but calculated ${rawPred.argmax} from rawPrediction.")
      val sum = rawPred.toArray.sum
      assert(Vectors.dense(rawPred.toArray.map(_ / sum)) === probPred,
        "probability prediction mismatch")
      assert(probPred.toArray.sum ~== 1.0 relTol 1E-5)
    }
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests of feature importance 特征重要性试验
  /////////////////////////////////////////////////////////////////////////////
  test("Feature importance with toy data") {//玩具数据的功能的重要性
    val numClasses = 2//numClasses 分类数
    val rf = new RandomForestClassifier()
      .setImpurity("Gini")//计算信息增益的准则
      .setMaxDepth(3)
      .setNumTrees(3)//
      .setFeatureSubsetStrategy("all")//每次分裂候选特征数量
      .setSubsamplingRate(1.0)//subsamplingRate学习一棵决策树使用的训练数据比例,范围[0,1]
      .setSeed(123)

    // In this data, feature 1 is very important.
    //在这个数据中,特征1是非常重要
    val data: RDD[LabeledPoint] = sc.parallelize(Seq(
      new LabeledPoint(0, Vectors.dense(1, 0, 0, 0, 1)),
      new LabeledPoint(1, Vectors.dense(1, 1, 0, 1, 0)),
      new LabeledPoint(1, Vectors.dense(1, 1, 0, 0, 0)),
      new LabeledPoint(0, Vectors.dense(1, 0, 0, 0, 0)),
      new LabeledPoint(1, Vectors.dense(1, 1, 0, 0, 0))
    ))
    val categoricalFeatures = Map.empty[Int, Int]
    val df: DataFrame = TreeTests.setMetadata(data, categoricalFeatures, numClasses)

    val importances = rf.fit(df).featureImportances
    val mostImportantFeature = importances.argmax
    assert(mostImportantFeature === 1)
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests of model save/load
  // 模型保存/加载测试
  /////////////////////////////////////////////////////////////////////////////

  // TODO: Reinstate test once save/load are implemented  SPARK-6725
  /*
  test("model save/load") {
    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString

    val trees =
      Range(0, 3).map(_ => OldDecisionTreeSuite.createModel(OldAlgo.Classification)).toArray
    val oldModel = new OldRandomForestModel(OldAlgo.Classification, trees)
    val newModel = RandomForestClassificationModel.fromOld(oldModel)

    // Save model, load it back, and compare.
    try {
      newModel.save(sc, path)
      val sameNewModel = RandomForestClassificationModel.load(sc, path)
      TreeTests.checkEqual(newModel, sameNewModel)
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }
  */
}

private object RandomForestClassifierSuite {

  /**
   * Train 2 models on the given dataset, one using the old API and one using the new API.
   * 给定数据集上训练2个模型,一个使用旧API,另一个使用新API
   * Convert the old model to the new format, compare them, and fail if they are not exactly equal.
   * 将旧模型转换为新格式,比较它们,如果它们不完全相等,则会失败
   */
  def compareAPIs(
      data: RDD[LabeledPoint],
      rf: RandomForestClassifier,
      categoricalFeatures: Map[Int, Int],
      numClasses: Int): Unit = {
    val oldStrategy =
      rf.getOldStrategy(categoricalFeatures, numClasses, OldAlgo.Classification, rf.getOldImpurity)
    val oldModel = OldRandomForest.trainClassifier(
    //getFeatureSubsetStrategy 每次分裂候选特征数量
      data, oldStrategy, rf.getNumTrees, rf.getFeatureSubsetStrategy, rf.getSeed.toInt)
    val newData: DataFrame = TreeTests.setMetadata(data, categoricalFeatures, numClasses)
    //fit()方法将DataFrame转化为一个Transformer的算法
    val newModel = rf.fit(newData)
    // Use parent from newTree since this is not checked anyways.
    val oldModelAsNew = RandomForestClassificationModel.fromOld(
      oldModel, newModel.parent.asInstanceOf[RandomForestClassifier], categoricalFeatures,
      numClasses)
    TreeTests.checkEqual(oldModelAsNew, newModel)
    assert(newModel.hasParent)
    assert(!newModel.trees.head.asInstanceOf[DecisionTreeClassificationModel].hasParent)
    assert(newModel.numClasses == numClasses)
  }
}
