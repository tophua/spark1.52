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
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{EnsembleTestHelper, GradientBoostedTrees => OldGBT}
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.util.Utils


/**
 * Test suite for [[GBTRegressor]].
 *  梯度提升树是一种决策树的集成算法,它通过反复迭代训练决策树来最小化损失函数。
 */
class GBTRegressorSuite extends SparkFunSuite with MLlibTestSparkContext {

  import GBTRegressorSuite.compareAPIs

  // Combinations for estimators, learning rates and subsamplingRate
  //组合估计,利率和subsamplingrate学习
  private val testCombinations =
    Array((10, 1.0, 1.0), (10, 0.1, 1.0), (10, 0.5, 0.75), (10, 0.1, 0.75))

  private var data: RDD[LabeledPoint] = _
  private var trainData: RDD[LabeledPoint] = _
  private var validationData: RDD[LabeledPoint] = _

  override def beforeAll() {
    super.beforeAll()
    data = sc.parallelize(EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 10, 100), 2)
    trainData =
      sc.parallelize(EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 20, 120), 2)
    validationData =
      sc.parallelize(EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 20, 80), 2)
  }
  //具有连续特征的回归:平方误差
  test("Regression with continuous features: SquaredError") {
    val categoricalFeatures = Map.empty[Int, Int]
    GBTRegressor.supportedLossTypes.foreach { loss =>
      testCombinations.foreach {
        case (maxIter, learningRate, subsamplingRate) =>
	        //梯度提升回归(GBT)
          val gbt = new GBTRegressor()
            .setMaxDepth(2)//树的最大深度
            .setSubsamplingRate(subsamplingRate)//学习一棵决策树使用的训练数据比例，范围[0,1]
            .setLossType(loss)//损失函数类型
            .setMaxIter(maxIter)//迭代次数
            .setStepSize(learningRate)//每次迭代优化步长
          compareAPIs(data, None, gbt, categoricalFeatures)
      }
    }
  }
  //GBTRegressor 数据合理的行为
  test("GBTRegressor behaves reasonably on toy data") {
    val df = sqlContext.createDataFrame(Seq(
      LabeledPoint(10, Vectors.dense(1, 2, 3, 4)),
      LabeledPoint(-5, Vectors.dense(6, 3, 2, 1)),
      LabeledPoint(11, Vectors.dense(2, 2, 3, 4)),
      LabeledPoint(-6, Vectors.dense(6, 4, 2, 1)),
      LabeledPoint(9, Vectors.dense(1, 2, 6, 4)),
      LabeledPoint(-4, Vectors.dense(6, 3, 2, 2))
    ))
    //梯度提升回归(GBT)
    val gbt = new GBTRegressor()
      .setMaxDepth(2)//树的最大深度
      .setMaxIter(2)//迭代次数
      //fit()方法将DataFrame转化为一个Transformer的算法
    val model = gbt.fit(df)

    // copied model must have the same parent.
    //复制的模型必须有相同的父
    MLTestingUtils.checkCopy(model)
     //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val preds = model.transform(df)
    val predictions = preds.select("prediction").map(_.getDouble(0))
    // Checks based on SPARK-8736 (to ensure it is not doing classification)
    assert(predictions.max() > 2)
    assert(predictions.min() < -1)
  }

  test("Checkpointing") {//检查点
    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString
    sc.setCheckpointDir(path)

    val df = sqlContext.createDataFrame(data)
    //梯度提升回归(GBT)
    val gbt = new GBTRegressor()
      .setMaxDepth(2)//树的最大深度
      .setMaxIter(5)//迭代次数
      .setStepSize(0.1)//每次迭代优化步长
      .setCheckpointInterval(2)//设置检查点间隔(>=1)
      //fit()方法将DataFrame转化为一个Transformer的算法
    val model = gbt.fit(df)

    sc.checkpointDir = None
    Utils.deleteRecursively(tempDir)

  }

  // TODO: Reinstate test once runWithValidation is implemented  SPARK-7132
  /*
  test("runWithValidation stops early and performs better on a validation dataset") {
    val categoricalFeatures = Map.empty[Int, Int]
    // Set maxIter large enough so that it stops early.
    val maxIter = 20
    //梯度提升回归(GBT)
    GBTRegressor.supportedLossTypes.foreach { loss =>
      val gbt = new GBTRegressor()//梯度提升回归(GBT)
        .setMaxIter(maxIter)
        .setMaxDepth(2)
        .setLossType(loss)
        .setValidationTol(0.0)
      compareAPIs(trainData, None, gbt, categoricalFeatures)
      compareAPIs(trainData, Some(validationData), gbt, categoricalFeatures)
    }
  }
  */

  /////////////////////////////////////////////////////////////////////////////
  // Tests of model save/load
  /////////////////////////////////////////////////////////////////////////////

  // TODO: Reinstate test once save/load are implemented  SPARK-6725
  /*
  test("model save/load") {
    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString

    val trees = Range(0, 3).map(_ => OldDecisionTreeSuite.createModel(OldAlgo.Regression)).toArray
    val treeWeights = Array(0.1, 0.3, 1.1)
    val oldModel = new OldGBTModel(OldAlgo.Regression, trees, treeWeights)
    val newModel = GBTRegressionModel.fromOld(oldModel)//梯度提升回归(GBT)

    // Save model, load it back, and compare.
    try {
      newModel.save(sc, path)
      //梯度提升回归(GBT)
      val sameNewModel = GBTRegressionModel.load(sc, path)
      TreeTests.checkEqual(newModel, sameNewModel)
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }
  */
}
//梯度提升回归(GBT)
private object GBTRegressorSuite {

  /**
   * Train 2 models on the given dataset, one using the old API and one using the new API.
   * 在给定的数据集上训练2个模型,一个使用旧的和一个使用新的
   * Convert the old model to the new format, compare them, and fail if they are not exactly equal.
   * 将旧的模型转换为新的格式，比较它们，如果它们不是完全相等的话，则失败了
   */
  def compareAPIs(
      data: RDD[LabeledPoint],
      validationData: Option[RDD[LabeledPoint]],
      gbt: GBTRegressor,
      categoricalFeatures: Map[Int, Int]): Unit = {
    val oldBoostingStrategy = gbt.getOldBoostingStrategy(categoricalFeatures, OldAlgo.Regression)
    val oldGBT = new OldGBT(oldBoostingStrategy)
    val oldModel = oldGBT.run(data)
    val newData: DataFrame = TreeTests.setMetadata(data, categoricalFeatures, numClasses = 0)
    val newModel = gbt.fit(newData)
    // Use parent from newTree since this is not checked anyways.
    val oldModelAsNew = GBTRegressionModel.fromOld(
      oldModel, newModel.parent.asInstanceOf[GBTRegressor], categoricalFeatures)
    TreeTests.checkEqual(oldModelAsNew, newModel)
  }
}
