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
import org.apache.spark.mllib.tree.{DecisionTree => OldDecisionTree, DecisionTreeSuite => OldDecisionTreeSuite}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
/**
 * 决策树分类套件
 */
class DecisionTreeClassifierSuite extends SparkFunSuite with MLlibTestSparkContext {

  import DecisionTreeClassifierSuite.compareAPIs

  private var categoricalDataPointsRDD: RDD[LabeledPoint] = _
  private var orderedLabeledPointsWithLabel0RDD: RDD[LabeledPoint] = _
  private var orderedLabeledPointsWithLabel1RDD: RDD[LabeledPoint] = _
  private var categoricalDataPointsForMulticlassRDD: RDD[LabeledPoint] = _
  private var continuousDataPointsForMulticlassRDD: RDD[LabeledPoint] = _
  private var categoricalDataPointsForMulticlassForOrderedFeaturesRDD: RDD[LabeledPoint] = _

  override def beforeAll() {
    super.beforeAll()
    categoricalDataPointsRDD =
      sc.parallelize(OldDecisionTreeSuite.generateCategoricalDataPoints())
    orderedLabeledPointsWithLabel0RDD =
      sc.parallelize(OldDecisionTreeSuite.generateOrderedLabeledPointsWithLabel0())
    orderedLabeledPointsWithLabel1RDD =
      sc.parallelize(OldDecisionTreeSuite.generateOrderedLabeledPointsWithLabel1())
    categoricalDataPointsForMulticlassRDD =
      sc.parallelize(OldDecisionTreeSuite.generateCategoricalDataPointsForMulticlass())
    continuousDataPointsForMulticlassRDD =
      sc.parallelize(OldDecisionTreeSuite.generateContinuousDataPointsForMulticlass())
    categoricalDataPointsForMulticlassForOrderedFeaturesRDD = sc.parallelize(
      OldDecisionTreeSuite.generateCategoricalDataPointsForMulticlassForOrderedFeatures())
  }
  //参数
  test("params") {
    ParamsSuite.checkParams(new DecisionTreeClassifier)
    val model = new DecisionTreeClassificationModel("dtc", new LeafNode(0.0, 0.0, null), 2)
    ParamsSuite.checkParams(model)
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests calling train()
  /////////////////////////////////////////////////////////////////////////////
  //具有序分类特征的二元分类方法
  test("Binary classification stump with ordered categorical features") {
    val dt = new DecisionTreeClassifier()
      .setImpurity("gini")//计算信息增益的准则
      .setMaxDepth(2)//树的最大深度
      .setMaxBins(100)//连续特征离散化的最大数量,以及选择每个节点分裂特征的方式
    val categoricalFeatures = Map(0 -> 3, 1-> 3)
    val numClasses = 2 //分类树数
    compareAPIs(categoricalDataPointsRDD, dt, categoricalFeatures, numClasses)
  }  
  //固定标签0.1熵的二进制分类的树,Gini
  test("Binary classification stump with fixed labels 0,1 for Entropy,Gini") {
    val dt = new DecisionTreeClassifier()
      .setMaxDepth(3)//树的最大深度
      .setMaxBins(100)//连续特征离散化的最大数量,以及选择每个节点分裂特征的方式
    val numClasses = 2
    Array(orderedLabeledPointsWithLabel0RDD, orderedLabeledPointsWithLabel1RDD).foreach { rdd =>
      DecisionTreeClassifier.supportedImpurities.foreach { impurity =>
        dt.setImpurity(impurity)//计算信息增益的准则
        compareAPIs(rdd, dt, categoricalFeatures = Map.empty[Int, Int], numClasses)
      }
    }
  }
  //多类分类的树和三元（无序）的分类特征
  test("Multiclass classification stump with 3-ary (unordered) categorical features") {
    val rdd = categoricalDataPointsForMulticlassRDD
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")//计算信息增益的准则
      .setMaxDepth(4)//树的最大深度
    val numClasses = 3
    val categoricalFeatures = Map(0 -> 3, 1 -> 3)
    compareAPIs(rdd, dt, categoricalFeatures, numClasses)
  }
  //有1个连续的特征分类,检查off-by-1误差
  test("Binary classification stump with 1 continuous feature, to check off-by-1 error") {
    val arr = Array(
      LabeledPoint(0.0, Vectors.dense(0.0)),
      LabeledPoint(1.0, Vectors.dense(1.0)),
      LabeledPoint(1.0, Vectors.dense(2.0)),
      LabeledPoint(1.0, Vectors.dense(3.0)))
    val rdd = sc.parallelize(arr)
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")//计算信息增益的准则
      .setMaxDepth(4)//树的最大深度
    val numClasses = 2
    compareAPIs(rdd, dt, categoricalFeatures = Map.empty[Int, Int], numClasses)
  }
  //具有2个连续特征的二叉分类
  test("Binary classification stump with 2 continuous features") {
    val arr = Array(
      LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 0.0)))),
      LabeledPoint(1.0, Vectors.sparse(2, Seq((1, 1.0)))),
      LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 0.0)))),
      LabeledPoint(1.0, Vectors.sparse(2, Seq((1, 2.0)))))
    val rdd = sc.parallelize(arr)
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")//计算信息增益的准则
      .setMaxDepth(4)//树的最大深度,默认值是 5
    val numClasses = 2
    compareAPIs(rdd, dt, categoricalFeatures = Map.empty[Int, Int], numClasses)
  }
  //多类分类的树和无序的分类特征,只要有足够的垃圾箱
  test("Multiclass classification stump with unordered categorical features," +
    " with just enough bins") {
    //离散连续性变量时最大的分箱数,默认是 32,理论上箱数越大粒度就越细,但是针对特定的数据集总有一个合理的箱数
    val maxBins = 2 * (math.pow(2, 3 - 1).toInt - 1) // just enough bins to allow unordered features
    val rdd = categoricalDataPointsForMulticlassRDD
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")//树节点选择的不纯度的衡量指标
      .setMaxDepth(4)//树的最大深度,默认值是 5
      .setMaxBins(maxBins)//离散连续性变量时最大的分箱数,默认是 32
    val categoricalFeatures = Map(0 -> 3, 1 -> 3)
    val numClasses = 3//随机森林需要训练的树的个数,默认值是 20
    compareAPIs(rdd, dt, categoricalFeatures, numClasses)
  }
  //多类分类的树和连续性的特点
  test("Multiclass classification stump with continuous features") {
    val rdd = continuousDataPointsForMulticlassRDD
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")//计算信息增益的准则
      .setMaxDepth(4)//最大深度
      .setMaxBins(100)//连续特征离散化的最大数量,以及选择每个节点分裂特征的方式
    val numClasses = 3
    compareAPIs(rdd, dt, categoricalFeatures = Map.empty[Int, Int], numClasses)
  }
  //多类分类的树和连续+无序分类特征
  test("Multiclass classification stump with continuous + unordered categorical features") {
    val rdd = continuousDataPointsForMulticlassRDD
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")//计算信息增益的准则
      .setMaxDepth(4)//最大深度
      .setMaxBins(100)//连续特征离散化的最大数量,以及选择每个节点分裂特征的方式
    val categoricalFeatures = Map(0 -> 3)
    val numClasses = 3//随机森林需要训练的树的个数，默认值是 20
    compareAPIs(rdd, dt, categoricalFeatures, numClasses)
  }
  //多类分类的树和10进制(有序)的分类特征
  test("Multiclass classification stump with 10-ary (ordered) categorical features") {
    val rdd = categoricalDataPointsForMulticlassForOrderedFeaturesRDD
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")//计算信息增益的准则
      .setMaxDepth(4)//最大深度
      .setMaxBins(100)//连续特征离散化的最大数量,以及选择每个节点分裂特征的方式
    val categoricalFeatures = Map(0 -> 10, 1 -> 10)
    val numClasses = 3//随机森林需要训练的树的个数，默认值是 20
    compareAPIs(rdd, dt, categoricalFeatures, numClasses)
  }
  //多类分类树与10叉(有序)的分类特征,只要有足够的垃圾箱
  test("Multiclass classification tree with 10-ary (ordered) categorical features," +
      " with just enough bins") {
    val rdd = categoricalDataPointsForMulticlassForOrderedFeaturesRDD
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")//计算信息增益的准则
      .setMaxDepth(4)//最大深度
      .setMaxBins(10)//连续特征离散化的最大数量,以及选择每个节点分裂特征的方式
    val categoricalFeatures = Map(0 -> 10, 1 -> 10)
    val numClasses = 3//随机森林需要训练的树的个数，默认值是 20
    compareAPIs(rdd, dt, categoricalFeatures, numClasses)
  }
  //分裂必须满足每个节点要求的最小实例
  test("split must satisfy min instances per node requirements") {
    val arr = Array(
      LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 0.0)))),
      LabeledPoint(1.0, Vectors.sparse(2, Seq((1, 1.0)))),
      LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 1.0)))))
    val rdd = sc.parallelize(arr)
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")//计算信息增益的准则
      .setMaxDepth(2)//树的最大深度
      .setMinInstancesPerNode(2)//分裂后自节点最少包含的实例数量
    val numClasses = 2//随机森林需要训练的树的个数，默认值是 20
    compareAPIs(rdd, dt, categoricalFeatures = Map.empty[Int, Int], numClasses)
  }  
  //不要选择不满足每个节点要求的最小实例的分割
  test("do not choose split that does not satisfy min instance per node requirements") {
    // if a split does not satisfy min instances per node requirements,
    //如果一个分裂不满足每个节点的要求的最小实例
    // this split is invalid, even though the information gain of split is large.
    //这种分裂是无效的，即使分裂的信息增益是大的
    val arr = Array(
      LabeledPoint(0.0, Vectors.dense(0.0, 1.0)),
      LabeledPoint(1.0, Vectors.dense(1.0, 1.0)),
      LabeledPoint(0.0, Vectors.dense(0.0, 0.0)),
      LabeledPoint(0.0, Vectors.dense(0.0, 0.0)))
    val rdd = sc.parallelize(arr)
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")//计算信息增益的准则
      .setMaxBins(2)//连续特征离散化的最大数量，以及选择每个节点分裂特征的方式
      .setMaxDepth(2)//树的最大深度
      .setMinInstancesPerNode(2)//分裂后自节点最少包含的实例数量
    val categoricalFeatures = Map(0 -> 2, 1-> 2)
    val numClasses = 2//随机森林需要训练的树的个数，默认值是 20
    compareAPIs(rdd, dt, categoricalFeatures, numClasses)
  }
  //分裂必须满足最小信息增益的要求
  test("split must satisfy min info gain requirements") {
    val arr = Array(
      LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 0.0)))),
      LabeledPoint(1.0, Vectors.sparse(2, Seq((1, 1.0)))),
      LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 1.0)))))
    val rdd = sc.parallelize(arr)

    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")//计算信息增益的准则
      .setMaxDepth(2)//树的最大深度
      .setMinInfoGain(1.0)//分裂节点时所需最小信息增益
    val numClasses = 2//随机森林需要训练的树的个数，默认值是 20
    compareAPIs(rdd, dt, categoricalFeatures = Map.empty[Int, Int], numClasses)
  }
  //预测原数据和预测概率
  test("predictRaw and predictProbability") {
    val rdd = continuousDataPointsForMulticlassRDD
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")//计算信息增益的准则
      .setMaxDepth(4)//树的最大深度
      .setMaxBins(100)//连续特征离散化的最大数量,以及选择每个节点分裂特征的方式
    val categoricalFeatures = Map(0 -> 3)
    val numClasses = 3//随机森林需要训练的树的个数，默认值是 20

    val newData: DataFrame = TreeTests.setMetadata(rdd, categoricalFeatures, numClasses)
    //fit()方法将DataFrame转化为一个Transformer的算法
    val newTree = dt.fit(newData)

    // copied model must have the same parent.
    //复制的模型必须有相同的父
    MLTestingUtils.checkCopy(newTree)
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val predictions = newTree.transform(newData)
    
    //getPredictionCol预测结果列名,getRawPredictionCol原始预测,getProbabilityCol类别条件概率预测结果列名
      .select(newTree.getPredictionCol, newTree.getRawPredictionCol, newTree.getProbabilityCol)
      .collect()

    predictions.foreach { case Row(pred: Double, rawPred: Vector, probPred: Vector) =>
      assert(pred === rawPred.argmax,
        s"Expected prediction $pred but calculated ${rawPred.argmax} from rawPrediction.")
      val sum = rawPred.toArray.sum
      assert(Vectors.dense(rawPred.toArray.map(_ / sum)) === probPred,
        "probability prediction mismatch")
    }
  }
  //训练一个类别分类特征
  test("training with 1-category categorical feature") {
    val data = sc.parallelize(Seq(
      LabeledPoint(0, Vectors.dense(0, 2, 3)),
      LabeledPoint(1, Vectors.dense(0, 3, 1)),
      LabeledPoint(0, Vectors.dense(0, 2, 2)),
      LabeledPoint(1, Vectors.dense(0, 3, 9)),
      LabeledPoint(0, Vectors.dense(0, 2, 6))
    ))
    val df = TreeTests.setMetadata(data, Map(0 -> 1), 2)
    //最大深度
    val dt = new DecisionTreeClassifier().setMaxDepth(3)
    //fit()方法将DataFrame转化为一个Transformer的算法
    val model = dt.fit(df)
    /*  println("rootNode:"+model.rootNode)
   println(model.labelCol.name+"\t"+model.labelCol.doc)
   println("getFeaturesCol:"+model.getFeaturesCol)*/
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests of model save/load
  /////////////////////////////////////////////////////////////////////////////

  // TODO: Reinstate test once save/load are implemented   SPARK-6725
  /*
  test("model save/load") {
    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString

    val oldModel = OldDecisionTreeSuite.createModel(OldAlgo.Classification)
    val newModel = DecisionTreeClassificationModel.fromOld(oldModel)

    // Save model, load it back, and compare.
    try {
      newModel.save(sc, path)
      val sameNewModel = DecisionTreeClassificationModel.load(sc, path)
      TreeTests.checkEqual(newModel, sameNewModel)
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }
  */
}

private[ml] object DecisionTreeClassifierSuite extends SparkFunSuite {

  /**
   * Train 2 decision trees on the given dataset, one using the old API and one using the new API.
   * 在给定数据集上训练2个决策树,一个使用旧的API和一个使用新的API
   * Convert the old tree to the new format, compare them, and fail if they are not exactly equal.
   * 将旧的树转换为新的格式,比较,失败如果他们不完全平等
   */
  def compareAPIs(
      data: RDD[LabeledPoint],
      dt: DecisionTreeClassifier,
      categoricalFeatures: Map[Int, Int],
      numClasses: Int): Unit = {
    val oldStrategy = dt.getOldStrategy(categoricalFeatures, numClasses)
    val oldTree = OldDecisionTree.train(data, oldStrategy)
    val newData: DataFrame = TreeTests.setMetadata(data, categoricalFeatures, numClasses)
    //fit()方法将DataFrame转化为一个Transformer的算法
    val newTree = dt.fit(newData)
    // Use parent from newTree since this is not checked anyways.
    val oldTreeAsNew = DecisionTreeClassificationModel.fromOld(
      oldTree, newTree.parent.asInstanceOf[DecisionTreeClassifier], categoricalFeatures)
    TreeTests.checkEqual(oldTreeAsNew, newTree)
  }
}
