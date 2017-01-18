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

package org.apache.spark.mllib.tree

import scala.collection.mutable

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.impl.DecisionTreeMetadata
import org.apache.spark.mllib.tree.impurity.{Gini, Variance}
import org.apache.spark.mllib.tree.model.{Node, RandomForestModel}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.util.Utils


/**
 * 随机森林算法是机器学习、计算机视觉等领域内应用极为广泛的一个算法,
 * 它不仅可以用来做分类,也可用来做回归即预测,随机森林机由多个决策树构成,
 * 相比于单个决策树算法,它分类、预测效果更好,不容易出现过度拟合的情况
 * RandomForest它们提供了随机森林具体的 trainClassifier
 * Test suite for [[RandomForest]].
 */
class RandomForestSuite extends SparkFunSuite with MLlibTestSparkContext {
  def binaryClassificationTestWithContinuousFeatures(strategy: Strategy) {
    val arr = EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 50, 1000)
    val rdd = sc.parallelize(arr)
    val numTrees = 1//训练的树的数量
    //featureSubsetStrategy="auto"特征子集采样策略,auto表示算法自主选取
    val rf = RandomForest.trainClassifier(rdd, strategy, numTrees = numTrees,
      featureSubsetStrategy = "auto", seed = 123)
    assert(rf.trees.size === 1)
    val rfTree = rf.trees(0)

    val dt = DecisionTree.train(rdd, strategy)

    EnsembleTestHelper.validateClassifier(rf, arr, 0.9)
    DecisionTreeSuite.validateClassifier(dt, arr, 0.9)

    // Make sure trees are the same.
    //确保树是相同的
    assert(rfTree.toString == dt.toString)
  }
  //具有连续特征的二元分类,相比决策树,随机森林(numTrees = 1)
  test("Binary classification with continuous features:" +
    " comparing DecisionTree vs. RandomForest(numTrees = 1)") {
      /**
      指明特征是类别型的以及每个类别型特征对应值(类别)。
      Map(0 -> 2, 4->10)表示特征0有两个特征值(0和1),特征4有10个特征值{0,1,2,3,…,9}。
      注意特征索引是从0开始的，0和4表示第1和第5个特征**/

    val categoricalFeaturesInfo = Map.empty[Int, Int]
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 2,
      numClasses = 2, categoricalFeaturesInfo = categoricalFeaturesInfo)
    binaryClassificationTestWithContinuousFeatures(strategy)
  }
  //具有连续特征和节点标识缓存的二元分类,相比决策树,随机森林(numTrees = 1)
  test("Binary classification with continuous features and node Id cache :" +
    " comparing DecisionTree vs. RandomForest(numTrees = 1)") {
    val categoricalFeaturesInfo = Map.empty[Int, Int]
    //树的最大深度,为了防止过拟合,设定划分的终止条件
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 2,
      numClasses = 2, categoricalFeaturesInfo = categoricalFeaturesInfo,
      useNodeIdCache = true)
    binaryClassificationTestWithContinuousFeatures(strategy)
  }

  def regressionTestWithContinuousFeatures(strategy: Strategy) {
    val arr = EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 50, 1000)
    val rdd = sc.parallelize(arr)
    val numTrees = 1
  //featureSubsetStrategy="auto"特征子集采样策略,auto表示算法自主选取
    val rf = RandomForest.trainRegressor(rdd, strategy, numTrees = numTrees,
      featureSubsetStrategy = "auto", seed = 123)
    assert(rf.trees.size === 1)//训练的树的数量
    val rfTree = rf.trees(0)

    val dt = DecisionTree.train(rdd, strategy)

    EnsembleTestHelper.validateRegressor(rf, arr, 0.01)
    DecisionTreeSuite.validateRegressor(dt, arr, 0.01)

    // Make sure trees are the same.
    //确保树是相同的
    assert(rfTree.toString == dt.toString)
  }
  //具有连续特征的回归,决策树,随机森林(numTrees = 1)
  test("Regression with continuous features:" +
    " comparing DecisionTree vs. RandomForest(numTrees = 1)") {
    val categoricalFeaturesInfo = Map.empty[Int, Int]
    val strategy = new Strategy(algo = Regression, impurity = Variance,
    //numClasses 分类数,maxBins连续特征离散化的最大数量,以及选择每个节点分裂特征的方式
    //树的最大深度,为了防止过拟合,设定划分的终止条件
      maxDepth = 2, maxBins = 10, numClasses = 2,
      categoricalFeaturesInfo = categoricalFeaturesInfo)
    regressionTestWithContinuousFeatures(strategy)
  }
  //连续特征和节点ID缓存的回归,决策树,随机森林(numTrees = 1)
  test("Regression with continuous features and node Id cache :" +
    " comparing DecisionTree vs. RandomForest(numTrees = 1)") {
    val categoricalFeaturesInfo = Map.empty[Int, Int]
   //树的最大深度,为了防止过拟合,设定划分的终止条件
    val strategy = new Strategy(algo = Regression, impurity = Variance,
      maxDepth = 2, maxBins = 10, numClasses = 2,
      categoricalFeaturesInfo = categoricalFeaturesInfo, useNodeIdCache = true)
    regressionTestWithContinuousFeatures(strategy)
  }

  def binaryClassificationTestWithContinuousFeaturesAndSubsampledFeatures(strategy: Strategy) {
    val numFeatures = 50
    val arr = EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures, 1000)
    val rdd = sc.parallelize(arr)

    // Select feature subset for top nodes.  Return true if OK.
    //选择顶部节点的特征子集,如果确定返回真
    def checkFeatureSubsetStrategy(
        numTrees: Int,//训练的树的数量
        featureSubsetStrategy: String,
        numFeaturesPerNode: Int): Unit = {
      val seeds = Array(123, 5354, 230, 349867, 23987)
      val maxMemoryUsage: Long = 128 * 1024L * 1024L
      val metadata =
        DecisionTreeMetadata.buildMetadata(rdd, strategy, numTrees, featureSubsetStrategy)
      seeds.foreach { seed =>
        val failString = s"Failed on test with:" +
          s"numTrees=$numTrees, featureSubsetStrategy=$featureSubsetStrategy," +
          s" numFeaturesPerNode=$numFeaturesPerNode, seed=$seed"
        val nodeQueue = new mutable.Queue[(Int, Node)]()
        val topNodes: Array[Node] = new Array[Node](numTrees)
        Range(0, numTrees).foreach { treeIndex =>
          topNodes(treeIndex) = Node.emptyNode(nodeIndex = 1)
          nodeQueue.enqueue((treeIndex, topNodes(treeIndex)))
        }
        val rng = new scala.util.Random(seed = seed)
        val (nodesForGroup: Map[Int, Array[Node]],
            treeToNodeToIndexInfo: Map[Int, Map[Int, RandomForest.NodeIndexInfo]]) =
          RandomForest.selectNodesToSplit(nodeQueue, maxMemoryUsage, metadata, rng)

        assert(nodesForGroup.size === numTrees, failString)
        //每棵树的1个节点
        assert(nodesForGroup.values.forall(_.size == 1), failString) // 1 node per tree

        if (numFeaturesPerNode == numFeatures) {
          // featureSubset values should all be None
          //特征子集值应该都是没有
          assert(treeToNodeToIndexInfo.values.forall(_.values.forall(_.featureSubset.isEmpty)),
            failString)
        } else {
          // Check number of features.
          //检查特征数
          assert(treeToNodeToIndexInfo.values.forall(_.values.forall(
            _.featureSubset.get.size === numFeaturesPerNode)), failString)
        }
      }
    }
    //auto自动,训练的树的数量
    checkFeatureSubsetStrategy(numTrees = 1, "auto", numFeatures)
    checkFeatureSubsetStrategy(numTrees = 1, "all", numFeatures)
     //math.sqrt返回数字的平方根
    checkFeatureSubsetStrategy(numTrees = 1, "sqrt", math.sqrt(numFeatures).ceil.toInt)
    //log2 对数
    checkFeatureSubsetStrategy(numTrees = 1, "log2",
      (math.log(numFeatures) / math.log(2)).ceil.toInt)
    checkFeatureSubsetStrategy(numTrees = 1, "onethird", (numFeatures / 3.0).ceil.toInt)

    checkFeatureSubsetStrategy(numTrees = 2, "all", numFeatures)
     //math.sqrt返回数字的平方根
    checkFeatureSubsetStrategy(numTrees = 2, "auto", math.sqrt(numFeatures).ceil.toInt)
     //math.sqrt返回数字的平方根
    checkFeatureSubsetStrategy(numTrees = 2, "sqrt", math.sqrt(numFeatures).ceil.toInt)
    checkFeatureSubsetStrategy(numTrees = 2, "log2",
      (math.log(numFeatures) / math.log(2)).ceil.toInt)
    checkFeatureSubsetStrategy(numTrees = 2, "onethird", (numFeatures / 3.0).ceil.toInt)
  }
  //具有连续特征的二元分类:抽样的特点
  test("Binary classification with continuous features: subsampling features") {
    val categoricalFeaturesInfo = Map.empty[Int, Int]
    //树的最大深度,为了防止过拟合,设定划分的终止条件
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 2,
      numClasses = 2, categoricalFeaturesInfo = categoricalFeaturesInfo)
    binaryClassificationTestWithContinuousFeaturesAndSubsampledFeatures(strategy)
  }
  //具有连续特征和节点标识缓存的二元分类:抽样的特点
  test("Binary classification with continuous features and node Id cache: subsampling features") {
    val categoricalFeaturesInfo = Map.empty[Int, Int]
    //树的最大深度,为了防止过拟合,设定划分的终止条件
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 2,
      numClasses = 2, categoricalFeaturesInfo = categoricalFeaturesInfo,
      useNodeIdCache = true)
    binaryClassificationTestWithContinuousFeaturesAndSubsampledFeatures(strategy)
  }
  //交替的分类和连续特征与多标签测试索引
  test("alternating categorical and continuous features with multiclass labels to test indexing") {
    val arr = new Array[LabeledPoint](4)
    //LabeledPoint标记点是局部向量,向量可以是密集型或者稀疏型,每个向量会关联了一个标签(label)
    arr(0) = new LabeledPoint(0.0, Vectors.dense(1.0, 0.0, 0.0, 3.0, 1.0))
    arr(1) = new LabeledPoint(1.0, Vectors.dense(0.0, 1.0, 1.0, 1.0, 2.0))
    arr(2) = new LabeledPoint(0.0, Vectors.dense(2.0, 0.0, 0.0, 6.0, 3.0))
    arr(3) = new LabeledPoint(2.0, Vectors.dense(0.0, 2.0, 1.0, 3.0, 2.0))
      /**
      指明特征是类别型的以及每个类别型特征对应值(类别)。
      Map(0 -> 2, 4->10)表示特征0有两个特征值(0和1),特征4有10个特征值{0,1,2,3,…,9}。
      注意特征索引是从0开始的，0和4表示第1和第5个特征**/
    val categoricalFeaturesInfo = Map(0 -> 3, 2 -> 2, 4 -> 4)
    val input = sc.parallelize(arr)
    //树的最大深度,为了防止过拟合,设定划分的终止条件
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 5,
      numClasses = 3, categoricalFeaturesInfo = categoricalFeaturesInfo)
    
    /**
     * 优化后的树结构
     * Tree 0:
        If (feature 3 <= 1.0)
         Predict: 1.0
        Else (feature 3 > 1.0)
         Predict: 0.0
  		Tree 1:
       If (feature 0 in {2.0})
         Predict: 0.0
        Else (feature 0 not in {2.0})
         If (feature 3 <= 1.0)
          Predict: 1.0
         Else (feature 3 > 1.0)
          Predict: 2.0
     */
    val model = RandomForest.trainClassifier(input, strategy, numTrees = 2,
       //sqrt 平方根函数
      featureSubsetStrategy = "sqrt", seed = 12345)
     // model.trees.foreach { x =>println(x.leftSide) }
      println(">>>"+model.trees.toList)
      println("toDebugString:"+model.toDebugString)
  }
  //随机森林在采样率
  test("subsampling rate in RandomForest"){
    val arr = EnsembleTestHelper.generateOrderedLabeledPoints(5, 20)
    val rdd = sc.parallelize(arr)
     /**
     * 1.训练数据集 
      2.目标类别个数，即结果有几种选择 
      3.Map中的键值分别对应Vector下标和该下标对应类别特征的取值情况，
        	空表示所有特征都是数值型（为了方便，示例中直接取空，实际当中并不能这么使用） 
      4.不纯性(impurity)度量：gini或者entropy，不纯度用来衡量一个规则的好坏，
      	好的规则可以将数据划分为等值的两部分，坏规则则相反 
      5.树的最大深度,为了防止过拟合,设定划分的终止条件
      6.决策树的最大桶数，每层使用的决策规则的个数，越多就可能精确，花费的时候也就越多,
      	最小的桶数应该不小于类别特征中最大的选择个数
     */
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 2,
      //numClasses 分类数
      numClasses = 2, categoricalFeaturesInfo = Map.empty[Int, Int],
      useNodeIdCache = true)

    val rf1 = RandomForest.trainClassifier(rdd, strategy, numTrees = 3,
      featureSubsetStrategy = "auto", seed = 123)
    strategy.subsamplingRate = 0.5//学习一棵决策树使用的训练数据比例，范围[0,1]
    val rf2 = RandomForest.trainClassifier(rdd, strategy, numTrees = 3,
      featureSubsetStrategy = "auto", seed = 123)
    assert(rf1.toDebugString != rf2.toDebugString)
  }

  test("model save/load") {//模型保存/加载
    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString

    Array(Classification, Regression).foreach { algo =>
      val trees = Range(0, 3).map(_ => DecisionTreeSuite.createModel(algo)).toArray
      val model = new RandomForestModel(algo, trees)

      // Save model, load it back, and compare.
      //保存模型,加载它回来,并比较
      try {
        model.save(sc, path)
        val sameModel = RandomForestModel.load(sc, path)
        assert(model.algo == sameModel.algo)
        model.trees.zip(sameModel.trees).foreach { case (treeA, treeB) =>
          DecisionTreeSuite.checkEqual(treeA, treeB)
        }
      } finally {
        Utils.deleteRecursively(tempDir)
      }
    }
  }

}
