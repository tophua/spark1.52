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

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.FeatureType._
import org.apache.spark.mllib.tree.configuration.{QuantileStrategy, Strategy}
import org.apache.spark.mllib.tree.impl.{BaggedPoint, DecisionTreeMetadata, TreePoint}
import org.apache.spark.mllib.tree.impurity.{Entropy, Gini, Variance}
import org.apache.spark.mllib.tree.model._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.util.Utils

/**
 *决策树是一个预测模型
 *决策树:分类与回归树(Classification and Regression Trees ,CART)算法常用于特征含有类别信息的分类或者回归问题
 * 特征值不标准化,优化需调整迭代次数
 */
class DecisionTreeSuite extends SparkFunSuite with MLlibTestSparkContext {

  /////////////////////////////////////////////////////////////////////////////
  // Tests examining individual elements of training
  // 测试训练各个元素
  /////////////////////////////////////////////////////////////////////////////
  //具有连续特征的二元分类:分裂和计算
  test("Binary classification with continuous features(连续特征): split and bin calculation") {
    /**
     * (1.0,[0.0,1.0]), (1.0,[0.0,1.0]), (1.0,[0.0,1.0]), (1.0,[0.0,1.0]), (1.0,[0.0,1.0]), (1.0,[0.0,1.0]), 
     * (1.0,[0.0,1.0]), (1.0,[0.0,1.0]), (1.0,[0.0,1.0]), (1.0,[0.0,1.0]), (1.0,[0.0,1.0]), (1.0,[0.0,1.0]), 
     * (1.0,[0.0,1.0]), (1.0,[0.0,1.0]), (1.0,[0.0,1.0]), (1.0,[0.0,1.0]), (1.0,[0.0,1.0]), (1.0,[0.0,1.0]), 
     * (1.0,[0.0,1.0]), (1.0,[0.0,1.0]), (1.0,[0.0,1.0]), (1.0,[0.0,1.0]), (1.0,[0.0,1.0]), (1.0,[0.0,1.0]), 
     */
    val arr = DecisionTreeSuite.generateOrderedLabeledPointsWithLabel1()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    //Gini基尼不纯度：将来自集合中的某种结果随机应用于集合中某一数据项的预期误差率。
    val strategy = new Strategy(Classification, Gini, 3, 2, 100)
    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(!metadata.isUnordered(featureIndex = 0))
    val (splits, bins) = DecisionTree.findSplitsBins(rdd, metadata)
    assert(splits.length === 2)//分裂
    assert(bins.length === 2)//桶数
    assert(splits(0).length === 99)
    assert(bins(0).length === 100)
  }
  //具有二元(有序)分类特征的二元分类:分裂和计算
  test("Binary classification with binary (ordered) categorical features:" +
    " split and bin calculation") {
    //[(1.0,[0.0,1.0]), (0.0,[1.0, 0.0]), (1.0,[0.0,1.0]), (0.0,[1.0, 0.0])]
    val arr = DecisionTreeSuite.generateCategoricalDataPoints()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    //策略
    val strategy = new Strategy(
      Classification,//分类
      Gini,
      maxDepth = 2,//树的最大深度,为了防止过拟合,设定划分的终止条件
      numClasses = 2,//numClasses 分类数
      maxBins = 100,//最大分箱数,当某个特征的特征值为连续时,该参数意思是将连续的特征值离散化为多少份
     /**
             指明特征的类别对应值(类别),注意特征索引是从0开始的,0和4表示第1和第5个特征
     Map(0 -> 2,4->10)表示特征0有两个特征值(0和1),特征4有10个特征值{0,1,2,3,…,9}             
     **/
      categoricalFeaturesInfo = Map(0 -> 2, 1-> 2))

    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    val (splits, bins) = DecisionTree.findSplitsBins(rdd, metadata)
    assert(!metadata.isUnordered(featureIndex = 0))
    assert(!metadata.isUnordered(featureIndex = 1))
    assert(splits.length === 2)
    assert(bins.length === 2)
    // no bins or splits pre-computed for ordered categorical features
    //没有垃圾箱或分割预先计算的有序分类功能
    assert(splits(0).length === 0)
    assert(bins(0).length === 0)
  }
  //采用三元二分类(有序)的分类特征:没有一个类别的样本
  test("Binary classification with 3-ary (ordered) categorical features," +
    " with no samples for one category") {
    val arr = DecisionTreeSuite.generateCategoricalDataPoints()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(
      Classification,
      Gini,
      maxDepth = 2,//树的最大深度,为了防止过拟合,设定划分的终止条件
      numClasses = 2,//numClasses 分类数
      maxBins = 100,//最大分箱数,当某个特征的特征值为连续时,该参数意思是将连续的特征值离散化为多少份
      //用Map存储类别(离散)特征及每个类别对应值(类别)的数量
      //例如 Map(n->k)表示特征n类别(离散)特征,特征值有K个,具体值为(0,1,...K-1)
      //Map中元素的键是特征在输入向量Vector中的下标,Map中元素的值是类别特征的不同取值个数
      //例如指定类别特征0取值3个数,指定类别1取值为3个数
      categoricalFeaturesInfo = Map(0 -> 3, 1 -> 3))

    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(!metadata.isUnordered(featureIndex = 0))
    assert(!metadata.isUnordered(featureIndex = 1))
    val (splits, bins) = DecisionTree.findSplitsBins(rdd, metadata)
    assert(splits.length === 2)
    assert(bins.length === 2)
    // no bins or splits pre-computed for ordered categorical features
    //没有垃圾箱或分割预先计算的有序分类功能
    assert(splits(0).length === 0)
    assert(bins(0).length === 0)
  }
  //从多类分类号提取类
  test("extract categories from a number for multiclass classification") {
    val l = DecisionTree.extractMultiClassCategories(13, 10)
    assert(l.length === 3)
    assert(List(3.0, 2.0, 0.0).toSeq === l.toSeq)
  }
  //查找拆分连续特征
  test("find splits for a continuous feature") {
    // find splits for normal case
    {
      val fakeMetadata = new DecisionTreeMetadata(1, 0, 0, 0,
        Map(), Set(),
        Array(6), Gini, QuantileStrategy.Sort,
        0, 0, 0.0, 0, 0
      )
      val featureSamples = Array.fill(200000)(math.random)
      val splits = DecisionTree.findSplitsForContinuousFeature(featureSamples, fakeMetadata, 0)
      assert(splits.length === 5)
      assert(fakeMetadata.numSplits(0) === 5)
      assert(fakeMetadata.numBins(0) === 6)
      // check returned splits are distinct
      //检查返回的拆分是不同的
      assert(splits.distinct.length === splits.length)
    }

    // find splits should not return identical splits
    //查找拆分不应该返回相同的拆分
    // when there are not enough split candidates, reduce the number of splits in metadata
    //当没有足够的分割候选时，减少元数据中的分裂次数
    {
      val fakeMetadata = new DecisionTreeMetadata(1, 0, 0, 0,
        Map(), Set(),
        Array(5), Gini, QuantileStrategy.Sort,
        0, 0, 0.0, 0, 0
      )
      val featureSamples = Array(1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 3).map(_.toDouble)
      val splits = DecisionTree.findSplitsForContinuousFeature(featureSamples, fakeMetadata, 0)
      assert(splits.length === 3)
      assert(fakeMetadata.numSplits(0) === 3)
      assert(fakeMetadata.numBins(0) === 4)
      // check returned splits are distinct
      //检查返回不同拆分
      assert(splits.distinct.length === splits.length)
    }

    // find splits when most samples close to the minimum
    //当大多数样本接近最小值时,发现分裂
    {
      val fakeMetadata = new DecisionTreeMetadata(1, 0, 0, 0,
        Map(), Set(),
        Array(3), Gini, QuantileStrategy.Sort,
        0, 0, 0.0, 0, 0
      )
      val featureSamples = Array(2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 4, 5).map(_.toDouble)
      val splits = DecisionTree.findSplitsForContinuousFeature(featureSamples, fakeMetadata, 0)
      assert(splits.length === 2)
      assert(fakeMetadata.numSplits(0) === 2)
      assert(fakeMetadata.numBins(0) === 3)
      assert(splits(0) === 2.0)
      assert(splits(1) === 3.0)
    }

    // find splits when most samples close to the maximum
    //当大多数样本接近最大值时发现分裂
    {
      val fakeMetadata = new DecisionTreeMetadata(1, 0, 0, 0,
        Map(), Set(),
        Array(3), Gini, QuantileStrategy.Sort,
        0, 0, 0.0, 0, 0
      )
      val featureSamples = Array(0, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2).map(_.toDouble)
      val splits = DecisionTree.findSplitsForContinuousFeature(featureSamples, fakeMetadata, 0)
      assert(splits.length === 1)
      assert(fakeMetadata.numSplits(0) === 1)
      assert(fakeMetadata.numBins(0) === 2)
      assert(splits(0) === 1.0)
    }
  }
  //无序多分类的分类特征:分裂和计算
  test("Multiclass classification with unordered(无序) categorical features:" +
      " split and bin calculations") {
    val arr = DecisionTreeSuite.generateCategoricalDataPoints()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(
      Classification,
      Gini,
      maxDepth = 2,//树的最大深度,为了防止过拟合,设定划分的终止条件
      numClasses = 100,//numClasses 分类数
      maxBins = 100,//最大分箱数,当某个特征的特征值为连续时,该参数意思是将连续的特征值离散化为多少份
      /**
      指明特征是类别型的以及每个类别型特征对应值(类别)。
      Map(0 -> 2, 4->10)表示特征0有两个特征值(0和1),特征4有10个特征值{0,1,2,3,…,9}。
      注意特征索引是从0开始的，0和4表示第1和第5个特征**/
      categoricalFeaturesInfo = Map(0 -> 3, 1-> 3))

    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    //特征无序
    assert(metadata.isUnordered(featureIndex = 0))
    assert(metadata.isUnordered(featureIndex = 1))
    //不同的策略采用不同的预测方法
    val (splits, bins) = DecisionTree.findSplitsBins(rdd, metadata)
    assert(splits.length === 2)
    assert(bins.length === 2)
    assert(splits(0).length === 3)
    assert(bins(0).length === 0)

    // Expecting 2^2 - 1 = 3 bins/splits
   /**
    * splits(0)
    * [
    * 	Feature = 0, threshold = -1.7976931348623157E308, featureType = Categorical, categories = List(0.0), 
    * 	Feature = 0, threshold = -1.7976931348623157E308, featureType = Categorical, categories = List(1.0), 
    * 	Feature = 0, threshold = -1.7976931348623157E308, featureType = Categorical, categories = List(1.0, 0.0)
    * ]
    */
    assert(splits(0)(0).feature === 0)
    //在二进制分类中设置阈值,范围为[0，1],如果类标签1的估计概率>Threshold,则预测1,否则0
    assert(splits(0)(0).threshold === Double.MinValue)
    assert(splits(0)(0).featureType === Categorical)
    assert(splits(0)(0).categories.length === 1)
    assert(splits(0)(0).categories.contains(0.0))
   /**
    * splits(1)
    * [
    *  Feature = 1, threshold = -1.7976931348623157E308, featureType = Categorical, categories = List(0.0),
    *  Feature = 1, threshold = -1.7976931348623157E308, featureType = Categorical, categories = List(1.0), 
    *  Feature = 1, threshold = -1.7976931348623157E308, featureType = Categorical, categories = List(1.0, 0.0)
    * ]
    */
    assert(splits(1)(0).feature === 1)
    //在二进制分类中设置阈值,范围为[0，1],如果类标签1的估计概率>Threshold,则预测1,否则0
    assert(splits(1)(0).threshold === Double.MinValue)
    assert(splits(1)(0).featureType === Categorical)
    assert(splits(1)(0).categories.length === 1)
    assert(splits(1)(0).categories.contains(0.0))

    assert(splits(0)(1).feature === 0)
    assert(splits(0)(1).threshold === Double.MinValue)
    assert(splits(0)(1).featureType === Categorical)
    assert(splits(0)(1).categories.length === 1)
    assert(splits(0)(1).categories.contains(1.0))
    assert(splits(1)(1).feature === 1)
    assert(splits(1)(1).threshold === Double.MinValue)
    assert(splits(1)(1).featureType === Categorical)
    assert(splits(1)(1).categories.length === 1)
    assert(splits(1)(1).categories.contains(1.0))

    assert(splits(0)(2).feature === 0)
    assert(splits(0)(2).threshold === Double.MinValue)
    assert(splits(0)(2).featureType === Categorical)
    assert(splits(0)(2).categories.length === 2)
    assert(splits(0)(2).categories.contains(0.0))
    assert(splits(0)(2).categories.contains(1.0))
    assert(splits(1)(2).feature === 1)
    assert(splits(1)(2).threshold === Double.MinValue)
    assert(splits(1)(2).featureType === Categorical)
    assert(splits(1)(2).categories.length === 2)
    assert(splits(1)(2).categories.contains(0.0))
    assert(splits(1)(2).categories.contains(1.0))

  }
  //有序分类特征的多类分类:分仓计算
  test("Multiclass classification with ordered categorical features: split and bin calculations") {
    val arr = DecisionTreeSuite.generateCategoricalDataPointsForMulticlassForOrderedFeatures()
    assert(arr.length === 3000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(
      Classification,
      Gini,
      maxDepth = 2,//树的最大深度,为了防止过拟合,设定划分的终止条件
      numClasses = 100,//numClasses 分类数
      maxBins = 100,//最大分箱数,当某个特征的特征值为连续时,该参数意思是将连续的特征值离散化为多少份
      /**
      指明特征是类别型的以及每个类别型特征对应值(类别)。
      Map(0 -> 2, 4->10)表示特征0有两个特征值(0和1),特征4有10个特征值{0,1,2,3,…,9}。
      注意特征索引是从0开始的，0和4表示第1和第5个特征**/
      categoricalFeaturesInfo = Map(0 -> 10, 1-> 10))
    //因此,分类的功能将被排序
    // 2^(10-1) - 1 > 100, so categorical features will be ordered

    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(!metadata.isUnordered(featureIndex = 0))
    assert(!metadata.isUnordered(featureIndex = 1))
    val (splits, bins) = DecisionTree.findSplitsBins(rdd, metadata)
    assert(splits.length === 2)
    assert(bins.length === 2)
    // no bins or splits pre-computed for ordered categorical features
    //没有垃圾箱或分割预先计算的有序分类功能
    assert(splits(0).length === 0)
    assert(bins(0).length === 0)
  }
  //避免在最后一级聚集
  test("Avoid aggregation on the last level") {
    val arr = Array(
    //LabeledPoint标记点是局部向量,向量可以是密集型或者稀疏型,每个向量会关联了一个标签(label)
      LabeledPoint(0.0, Vectors.dense(1.0, 0.0, 0.0)),
      LabeledPoint(1.0, Vectors.dense(0.0, 1.0, 1.0)),
      LabeledPoint(0.0, Vectors.dense(2.0, 0.0, 0.0)),
      LabeledPoint(1.0, Vectors.dense(0.0, 2.0, 1.0)))
    val input = sc.parallelize(arr)

    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 1,
        //categoricalFeaturesInfo 指明哪些特征是类别型的以及每个类别型特征对应值(类别)的数量,
	//通过map来指定,map的key是特征索引,value是特征值数量
        //numClasses 分类数
      numClasses = 2, categoricalFeaturesInfo = Map(0 -> 3))
    val metadata = DecisionTreeMetadata.buildMetadata(input, strategy)
    val (splits, bins) = DecisionTree.findSplitsBins(input, metadata)

    val treeInput = TreePoint.convertToTreeRDD(input, bins, metadata)
    val baggedInput = BaggedPoint.convertToBaggedRDD(treeInput, 1.0, 1, false)

    val topNode = Node.emptyNode(nodeIndex = 1)
    assert(topNode.predict.predict === Double.MinValue)
    assert(topNode.impurity === -1.0)
    assert(topNode.isLeaf === false)

    val nodesForGroup = Map((0, Array(topNode)))
    val treeToNodeToIndexInfo = Map((0, Map(
      (topNode.id, new RandomForest.NodeIndexInfo(0, None))
    )))
    val nodeQueue = new mutable.Queue[(Int, Node)]()
    DecisionTree.findBestSplits(baggedInput, metadata, Array(topNode),
      nodesForGroup, treeToNodeToIndexInfo, splits, bins, nodeQueue)

    // don't enqueue leaf nodes into node queue
    //不要将叶子节点到节点的队列
    assert(nodeQueue.isEmpty)

    // set impurity and predict for topNode
    //设置不纯度和预测topnode
    assert(topNode.predict.predict !== Double.MinValue)
    assert(topNode.impurity !== -1.0)

    // set impurity and predict for child nodes
    assert(topNode.leftNode.get.predict.predict === 0.0)
    assert(topNode.rightNode.get.predict.predict === 1.0)
    assert(topNode.leftNode.get.impurity === 0.0)//不纯度
    assert(topNode.rightNode.get.impurity === 0.0)//不纯度
  }
  //避免聚合,如果不纯度是0
  test("Avoid aggregation if impurity is 0.0") {
    val arr = Array(
    //LabeledPoint标记点是局部向量,向量可以是密集型或者稀疏型,每个向量会关联了一个标签(label)
      LabeledPoint(0.0, Vectors.dense(1.0, 0.0, 0.0)),
      LabeledPoint(1.0, Vectors.dense(0.0, 1.0, 1.0)),
      LabeledPoint(0.0, Vectors.dense(2.0, 0.0, 0.0)),
      LabeledPoint(1.0, Vectors.dense(0.0, 2.0, 1.0)))
    val input = sc.parallelize(arr)
    //numClasses 分类数
    //categoricalFeaturesInfo 指明哪些特征是类别型的以及每个类别型特征对应值(类别)的数量,
    //通过map来指定,map的key是特征索引,value是特征值数量
    //树的最大深度,为了防止过拟合,设定划分的终止条件
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 5,
      numClasses = 2, categoricalFeaturesInfo = Map(0 -> 3))
    val metadata = DecisionTreeMetadata.buildMetadata(input, strategy)
    val (splits, bins) = DecisionTree.findSplitsBins(input, metadata)

    val treeInput = TreePoint.convertToTreeRDD(input, bins, metadata)
    val baggedInput = BaggedPoint.convertToBaggedRDD(treeInput, 1.0, 1, false)

    val topNode = Node.emptyNode(nodeIndex = 1)
    assert(topNode.predict.predict === Double.MinValue)
    assert(topNode.impurity === -1.0)
    assert(topNode.isLeaf === false)

    val nodesForGroup = Map((0, Array(topNode)))
    val treeToNodeToIndexInfo = Map((0, Map(
      (topNode.id, new RandomForest.NodeIndexInfo(0, None))
    )))
    val nodeQueue = new mutable.Queue[(Int, Node)]()
    DecisionTree.findBestSplits(baggedInput, metadata, Array(topNode),
      nodesForGroup, treeToNodeToIndexInfo, splits, bins, nodeQueue)

    // don't enqueue a node into node queue if its impurity is 0.0
    //不要将一个节点到节点的队列，如果不纯度0
    assert(nodeQueue.isEmpty)

    // set impurity and predict for topNode
    //设置不纯度和预测顶节点
    assert(topNode.predict.predict !== Double.MinValue)
    assert(topNode.impurity !== -1.0)

    // set impurity and predict for child nodes
    //设置不纯度和预测子节点
    assert(topNode.leftNode.get.predict.predict === 0.0)
    assert(topNode.rightNode.get.predict.predict === 1.0)
    assert(topNode.leftNode.get.impurity === 0.0)
    assert(topNode.rightNode.get.impurity === 0.0)
  }
  //第二级节点构建与无组
  test("Second level node building with vs. without groups") {
    val arr = DecisionTreeSuite.generateOrderedLabeledPoints()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(Classification, Entropy, 3, 2, 100)
    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    val (splits, bins) = DecisionTree.findSplitsBins(rdd, metadata)
    assert(splits.length === 2)
    assert(splits(0).length === 99)
    assert(bins.length === 2)
    assert(bins(0).length === 100)

    // Train a 1-node model
    //训练一个一级节点
    //熵:代表集合的无序程度
    //树的最大深度,为了防止过拟合,设定划分的终止条件
    val strategyOneNode = new Strategy(Classification, Entropy, maxDepth = 1,
    //numClasses 分类数
    //最大分箱数,当某个特征的特征值为连续时,该参数意思是将连续的特征值离散化为多少份
      numClasses = 2, maxBins = 100)
    val modelOneNode = DecisionTree.train(rdd, strategyOneNode)
    val rootNode1 = modelOneNode.topNode.deepCopy()
    val rootNode2 = modelOneNode.topNode.deepCopy()
    assert(rootNode1.leftNode.nonEmpty)
    assert(rootNode1.rightNode.nonEmpty)

    val treeInput = TreePoint.convertToTreeRDD(rdd, bins, metadata)
    val baggedInput = BaggedPoint.convertToBaggedRDD(treeInput, 1.0, 1, false)

    // Single group second level tree construction.
    //单分组二级树结构
    val nodesForGroup = Map((0, Array(rootNode1.leftNode.get, rootNode1.rightNode.get)))
    val treeToNodeToIndexInfo = Map((0, Map(
      (rootNode1.leftNode.get.id, new RandomForest.NodeIndexInfo(0, None)),
      (rootNode1.rightNode.get.id, new RandomForest.NodeIndexInfo(1, None)))))
    val nodeQueue = new mutable.Queue[(Int, Node)]()
    DecisionTree.findBestSplits(baggedInput, metadata, Array(rootNode1),
      nodesForGroup, treeToNodeToIndexInfo, splits, bins, nodeQueue)
    val children1 = new Array[Node](2)
    children1(0) = rootNode1.leftNode.get
    children1(1) = rootNode1.rightNode.get

    // Train one second-level node at a time.
    //一次训练一个二级节点
    val nodesForGroupA = Map((0, Array(rootNode2.leftNode.get)))
    val treeToNodeToIndexInfoA = Map((0, Map(
      (rootNode2.leftNode.get.id, new RandomForest.NodeIndexInfo(0, None)))))
    nodeQueue.clear()
    DecisionTree.findBestSplits(baggedInput, metadata, Array(rootNode2),
      nodesForGroupA, treeToNodeToIndexInfoA, splits, bins, nodeQueue)
    val nodesForGroupB = Map((0, Array(rootNode2.rightNode.get)))
    val treeToNodeToIndexInfoB = Map((0, Map(
      (rootNode2.rightNode.get.id, new RandomForest.NodeIndexInfo(0, None)))))
    nodeQueue.clear()
    DecisionTree.findBestSplits(baggedInput, metadata, Array(rootNode2),
      nodesForGroupB, treeToNodeToIndexInfoB, splits, bins, nodeQueue)
    val children2 = new Array[Node](2)
    children2(0) = rootNode2.leftNode.get
    children2(1) = rootNode2.rightNode.get

    // Verify whether the splits obtained using single group and multiple group level
    // construction strategies are the same.
    //验证是否相同是否使用单个组和多组级别的构建策略
    for (i <- 0 until 2) {
      assert(children1(i).stats.nonEmpty && children1(i).stats.get.gain > 0)
      assert(children2(i).stats.nonEmpty && children2(i).stats.get.gain > 0)
      assert(children1(i).split === children2(i).split)
      assert(children1(i).stats.nonEmpty && children2(i).stats.nonEmpty)
      val stats1 = children1(i).stats.get
      val stats2 = children2(i).stats.get
      assert(stats1.gain === stats2.gain)
      assert(stats1.impurity === stats2.impurity)
      assert(stats1.leftImpurity === stats2.leftImpurity)
      assert(stats1.rightImpurity === stats2.rightImpurity)
      assert(children1(i).predict.predict === children2(i).predict.predict)
    }
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests calling train() 测试调用训练
  /////////////////////////////////////////////////////////////////////////////
  //具有有序分类特征的二元分类方法
  test("Binary classification stump with ordered categorical features") {
    val arr = DecisionTreeSuite.generateCategoricalDataPoints()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(
      Classification,
      Gini,
      numClasses = 2,//分类数
      maxDepth = 2,//树的最大深度,为了防止过拟合,设定划分的终止条件
      //最大分箱数,当某个特征的特征值为连续时,该参数意思是将连续的特征值离散化为多少份
      maxBins = 100,
      /**
     指明特征的类别对应值(类别),注意特征索引是从0开始的,0和4表示第1和第5个特征
     Map(0 -> 2,4->10)表示特征0有两个特征值(0和1),特征4有10个特征值{0,1,2,3,…,9}             
     **/
      categoricalFeaturesInfo = Map(0 -> 3, 1-> 3))

    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(!metadata.isUnordered(featureIndex = 0))
    assert(!metadata.isUnordered(featureIndex = 1))
    val (splits, bins) = DecisionTree.findSplitsBins(rdd, metadata)
    assert(splits.length === 2)
    assert(bins.length === 2)
    // no bins or splits pre-computed for ordered categorical features
    //没有垃圾箱或分割预先计算的有序分类功能
    assert(splits(0).length === 0)
    assert(bins(0).length === 0)

    val rootNode = DecisionTree.train(rdd, strategy).topNode

    val split = rootNode.split.get
    //分类
    assert(split.categories === List(1.0))
    //特征类型
    assert(split.featureType === Categorical)
    assert(split.threshold === Double.MinValue)

    val stats = rootNode.stats.get
    assert(stats.gain > 0)
    assert(rootNode.predict.predict === 1)
    assert(stats.impurity > 0.2)
  }  
  //三元回归树桩(有序)的类别特征
  test("Regression stump with 3-ary (ordered) categorical features") {
    val arr = DecisionTreeSuite.generateCategoricalDataPoints()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(
      Regression,
      Variance,
      maxDepth = 2,//树的最大深度,为了防止过拟合,设定划分的终止条件
      //最大分箱数,当某个特征的特征值为连续时,该参数意思是将连续的特征值离散化为多少份
      maxBins = 100,
      categoricalFeaturesInfo = Map(0 -> 3, 1-> 3))

    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(!metadata.isUnordered(featureIndex = 0))
    assert(!metadata.isUnordered(featureIndex = 1))

    val rootNode = DecisionTree.train(rdd, strategy).topNode

    val split = rootNode.split.get
    assert(split.categories.length === 1)
    assert(split.categories.contains(1.0))
    assert(split.featureType === Categorical)
    assert(split.threshold === Double.MinValue)

    val stats = rootNode.stats.get
    assert(stats.gain > 0)
    assert(rootNode.predict.predict === 0.6)
    assert(stats.impurity > 0.2)
  }
  //具有二元(有序)分类特征的回归分析
  test("Regression stump with binary (ordered) categorical features") {
    val arr = DecisionTreeSuite.generateCategoricalDataPoints()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(
      Regression,
      Variance,
      maxDepth = 2,//树的最大深度,为了防止过拟合,设定划分的终止条件
      maxBins = 100,//连续特征离散化的最大数量,以及选择每个节点分裂特征的方式
      categoricalFeaturesInfo = Map(0 -> 2, 1-> 2))
    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(!metadata.isUnordered(featureIndex = 0))
    assert(!metadata.isUnordered(featureIndex = 1))

    val model = DecisionTree.train(rdd, strategy)
    DecisionTreeSuite.validateRegressor(model, arr, 0.0)
    assert(model.numNodes === 3)
    assert(model.depth === 1)
  }
  //Gini的固定标签0的二元分类
  test("Binary classification stump with fixed label 0 for Gini") {
    val arr = DecisionTreeSuite.generateOrderedLabeledPointsWithLabel0()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    //maxBins最大分箱数,当某个特征的特征值为连续时,该参数意思是将连续的特征值离散化为多少份
    //树的最大深度,为了防止过拟合,设定划分的终止条件
    val strategy = new Strategy(Classification, Gini, maxDepth = 3,
      numClasses = 2, maxBins = 100)
    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(!metadata.isUnordered(featureIndex = 0))
    assert(!metadata.isUnordered(featureIndex = 1))

    val (splits, bins) = DecisionTree.findSplitsBins(rdd, metadata)
    assert(splits.length === 2)
    assert(splits(0).length === 99)
    assert(bins.length === 2)
    assert(bins(0).length === 100)

    val rootNode = DecisionTree.train(rdd, strategy).topNode

    val stats = rootNode.stats.get
    assert(stats.gain === 0)
    assert(stats.leftImpurity === 0)
    assert(stats.rightImpurity === 0)
  }
  //Gini的固定标签1的二元分类
  test("Binary classification stump with fixed label 1 for Gini") {
    val arr = DecisionTreeSuite.generateOrderedLabeledPointsWithLabel1()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    //maxBins连续特征离散化的最大数量,以及选择每个节点分裂特征的方式
    //树的最大深度,为了防止过拟合,设定划分的终止条件
    val strategy = new Strategy(Classification, Gini, maxDepth = 3,
      numClasses = 2, maxBins = 100)
    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(!metadata.isUnordered(featureIndex = 0))
    assert(!metadata.isUnordered(featureIndex = 1))

    val (splits, bins) = DecisionTree.findSplitsBins(rdd, metadata)
    assert(splits.length === 2)
    assert(splits(0).length === 99)
    assert(bins.length === 2)
    assert(bins(0).length === 100)

    val rootNode = DecisionTree.train(rdd, strategy).topNode

    val stats = rootNode.stats.get
    assert(stats.gain === 0)
    assert(stats.leftImpurity === 0)
    assert(stats.rightImpurity === 0)
    assert(rootNode.predict.predict === 1)
  }
  //具有固定标签0的熵的二元分类
  test("Binary classification stump with fixed label 0 for Entropy") {
    val arr = DecisionTreeSuite.generateOrderedLabeledPointsWithLabel0()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    //numClasses 分类数
    //树的最大深度,为了防止过拟合,设定划分的终止条件
    val strategy = new Strategy(Classification, Entropy, maxDepth = 3,
      numClasses = 2, maxBins = 100)
    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(!metadata.isUnordered(featureIndex = 0))
    assert(!metadata.isUnordered(featureIndex = 1))

    val (splits, bins) = DecisionTree.findSplitsBins(rdd, metadata)
    assert(splits.length === 2)
    assert(splits(0).length === 99)
    assert(bins.length === 2)
    assert(bins(0).length === 100)

    val rootNode = DecisionTree.train(rdd, strategy).topNode

    val stats = rootNode.stats.get
    assert(stats.gain === 0)
    assert(stats.leftImpurity === 0)
    assert(stats.rightImpurity === 0)
    assert(rootNode.predict.predict === 0)
  }
  //具有固定标签1的熵的二元分类
  test("Binary classification stump with fixed label 1 for Entropy") {
    val arr = DecisionTreeSuite.generateOrderedLabeledPointsWithLabel1()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    //树的最大深度,为了防止过拟合,设定划分的终止条件
    val strategy = new Strategy(Classification, Entropy, maxDepth = 3,
      numClasses = 2, maxBins = 100)
    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(!metadata.isUnordered(featureIndex = 0))
    assert(!metadata.isUnordered(featureIndex = 1))

    val (splits, bins) = DecisionTree.findSplitsBins(rdd, metadata)
    assert(splits.length === 2)
    assert(splits(0).length === 99)
    assert(bins.length === 2)
    assert(bins(0).length === 100)

    val rootNode = DecisionTree.train(rdd, strategy).topNode

    val stats = rootNode.stats.get
    assert(stats.gain === 0)
    assert(stats.leftImpurity === 0)
    assert(stats.rightImpurity === 0)
    assert(rootNode.predict.predict === 1)
  }
  //多类分类的树和三元(无序)的分类特征
  test("Multiclass classification stump with 3-ary (unordered) categorical features") {
    val arr = DecisionTreeSuite.generateCategoricalDataPointsForMulticlass()
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 4,
        //categoricalFeaturesInfo用Map存储类别(离散)特征及每个类别对应值(类别)的数量
      //例如 Map(n->k)表示特征n类别(离散)特征,特征值有K个,具体值为(0,1,...K-1)
      numClasses = 3, categoricalFeaturesInfo = Map(0 -> 3, 1 -> 3))
    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(strategy.isMulticlassClassification)
    assert(metadata.isUnordered(featureIndex = 0))
    assert(metadata.isUnordered(featureIndex = 1))

    val rootNode = DecisionTree.train(rdd, strategy).topNode

    val split = rootNode.split.get
    assert(split.feature === 0)
    assert(split.categories.length === 1)
    assert(split.categories.contains(1))
    assert(split.featureType === Categorical)
  }
  //有1个连续的特征分类,检查off-by-1误差
  test("Binary classification stump with 1 continuous feature, to check off-by-1 error") {
    val arr = Array(
    //LabeledPoint标记点是局部向量,向量可以是密集型或者稀疏型,每个向量会关联了一个标签(label)
      LabeledPoint(0.0, Vectors.dense(0.0)),
      LabeledPoint(1.0, Vectors.dense(1.0)),
      LabeledPoint(1.0, Vectors.dense(2.0)),
      LabeledPoint(1.0, Vectors.dense(3.0)))
    val rdd = sc.parallelize(arr)
    //树的最大深度,为了防止过拟合,设定划分的终止条件
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 4,
      numClasses = 2)

    val model = DecisionTree.train(rdd, strategy)
    DecisionTreeSuite.validateClassifier(model, arr, 1.0)
    assert(model.numNodes === 3)
    assert(model.depth === 1)
  }
  //具有2个连续特征的二叉分类
  test("Binary classification stump with 2 continuous features") {
    val arr = Array(
      LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 0.0)))),
      LabeledPoint(1.0, Vectors.sparse(2, Seq((1, 1.0)))),
      LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 0.0)))),
      LabeledPoint(1.0, Vectors.sparse(2, Seq((1, 2.0)))))

    val rdd = sc.parallelize(arr)
    /**
    1.训练数据集 
    2.目标类别个数，即结果有几种选择 
    3.Map中的键值分别对应Vector下标和该下标对应类别特征的取值情况，
               空表示所有特征都是数值型（为了方便，示例中直接取空，实际当中并不能这么使用） 
    4.不纯性(impurity)度量：gini或者entropy，不纯度用来衡量一个规则的好坏，
               好的规则可以将数据划分为等值的两部分，坏规则则相反 
    5.决策树的最大深度，越深的决策树越有可能产生过度拟合的问题 
    6.决策树的最大桶数，每层使用的决策规则的个数，越多就可能精确，花费的时候也就越多,
                 最小的桶数应该不小于类别特征中最大的选择个数
     */
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 4,
      numClasses = 2)

    val model = DecisionTree.train(rdd, strategy)
    DecisionTreeSuite.validateClassifier(model, arr, 1.0)
    assert(model.numNodes === 3)
    assert(model.depth === 1)
    assert(model.topNode.split.get.feature === 1)
  }
  //多类分类的树桩和无序的分类特征
  test("Multiclass classification stump with unordered categorical features," +
    " with just enough bins") {
    //足够的垃圾箱允许无序的特征
    val maxBins = 2 * (math.pow(2, 3 - 1).toInt - 1) // just enough bins to allow unordered features
    val arr = DecisionTreeSuite.generateCategoricalDataPointsForMulticlass()
    val rdd = sc.parallelize(arr)
    //树的最大深度（>=0）
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 4,
    //numClasses 分类数
    //maxBins连续特征离散化的最大数量,以及选择每个节点分裂特征的方式
      numClasses = 3, maxBins = maxBins,
     /**
     指明特征的类别对应值(类别),注意特征索引是从0开始的,0和4表示第1和第5个特征
     Map(0 -> 2,4->10)表示特征0有两个特征值(0和1),特征4有10个特征值{0,1,2,3,…,9}             
     **/
      categoricalFeaturesInfo = Map(0 -> 3, 1 -> 3))
    assert(strategy.isMulticlassClassification)
    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(metadata.isUnordered(featureIndex = 0))
    assert(metadata.isUnordered(featureIndex = 1))

    val model = DecisionTree.train(rdd, strategy)
    DecisionTreeSuite.validateClassifier(model, arr, 1.0)
    assert(model.numNodes === 3)
    assert(model.depth === 1)

    val rootNode = model.topNode

    val split = rootNode.split.get
    assert(split.feature === 0)
    assert(split.categories.length === 1)
    assert(split.categories.contains(1))
    assert(split.featureType === Categorical)

    val gain = rootNode.stats.get
    assert(gain.leftImpurity === 0)
    assert(gain.rightImpurity === 0)
  }  
  //多类分类的连续性的特征
  test("Multiclass classification stump with continuous features") {
    val arr = DecisionTreeSuite.generateContinuousDataPointsForMulticlass()
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 4,
    //numClasses 分类数
      numClasses = 3, maxBins = 100)
    assert(strategy.isMulticlassClassification)
    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)

    val model = DecisionTree.train(rdd, strategy)
    DecisionTreeSuite.validateClassifier(model, arr, 0.9)

    val rootNode = model.topNode

    val split = rootNode.split.get
    assert(split.feature === 1)
    assert(split.featureType === Continuous)
    //在二进制分类中设置阈值,范围为[0，1],如果类标签1的估计概率>Threshold,则预测1,否则0
    assert(split.threshold > 1980)
    assert(split.threshold < 2020)

  }
  //多类分类连续+无序分类特征
  test("Multiclass classification stump with continuous + unordered categorical features") {
    val arr = DecisionTreeSuite.generateContinuousDataPointsForMulticlass()
    val rdd = sc.parallelize(arr)//树的最大深度,为了防止过拟合,设定划分的终止条件
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 4,
      numClasses = 3, maxBins = 100, categoricalFeaturesInfo = Map(0 -> 3))
    assert(strategy.isMulticlassClassification)
    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(metadata.isUnordered(featureIndex = 0))

    val model = DecisionTree.train(rdd, strategy)
    DecisionTreeSuite.validateClassifier(model, arr, 0.9)

    val rootNode = model.topNode

    val split = rootNode.split.get
    assert(split.feature === 1)
    assert(split.featureType === Continuous)
    //在二进制分类中设置阈值,范围为[0，1],如果类标签1的估计概率>Threshold,则预测1,否则0
    assert(split.threshold > 1980)
    assert(split.threshold < 2020)
  }
  //多类分类和10进制(有序)的分类特征
  test("Multiclass classification stump with 10-ary (ordered) categorical features") {
    val arr = DecisionTreeSuite.generateCategoricalDataPointsForMulticlassForOrderedFeatures()
    val rdd = sc.parallelize(arr)//树的最大深度,为了防止过拟合,设定划分的终止条件
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 4,
      numClasses = 3, maxBins = 100,
      categoricalFeaturesInfo = Map(0 -> 10, 1 -> 10))
    assert(strategy.isMulticlassClassification)
    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(!metadata.isUnordered(featureIndex = 0))
    assert(!metadata.isUnordered(featureIndex = 1))

    val rootNode = DecisionTree.train(rdd, strategy).topNode

    val split = rootNode.split.get
    assert(split.feature === 0)
    assert(split.categories.length === 1)
    assert(split.categories.contains(1.0))
    assert(split.featureType === Categorical)
  }  
  //多类分类树与10(有序)的分类特征:只要有足够的垃圾箱
  test("Multiclass classification tree with 10-ary (ordered) categorical features," +
      " with just enough bins") {
    val arr = DecisionTreeSuite.generateCategoricalDataPointsForMulticlassForOrderedFeatures()
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 4,
    //numClasses 分类数,maxBins连续特征离散化的最大数量,以及选择每个节点分裂特征的方式
      numClasses = 3, maxBins = 10,
      categoricalFeaturesInfo = Map(0 -> 10, 1 -> 10))
    assert(strategy.isMulticlassClassification)

    val model = DecisionTree.train(rdd, strategy)
    DecisionTreeSuite.validateClassifier(model, arr, 0.6)
  }
  //分裂必须满足每个节点要求的最小实例
  test("split must satisfy min instances per node requirements") {
    val arr = Array(
    //LabeledPoint标记点是局部向量,向量可以是密集型或者稀疏型,每个向量会关联了一个标签(label)
      LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 0.0)))),
      LabeledPoint(1.0, Vectors.sparse(2, Seq((1, 1.0)))),
      LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 1.0)))))
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(algo = Classification, impurity = Gini,
    //minInstancesPerNode 切分后每个子节点至少包含的样本实例数,否则停止切分,于终止迭代计算
      maxDepth = 2, numClasses = 2, minInstancesPerNode = 2)

    val model = DecisionTree.train(rdd, strategy)
    assert(model.topNode.isLeaf)
    assert(model.topNode.predict.predict == 0.0)
    val predicts = rdd.map(p => model.predict(p.features)).collect()
    predicts.foreach { predict =>
      assert(predict == 0.0)
    }

    // test when no valid split can be found
    //测试时,没有有效的分裂可以被发现
    val rootNode = model.topNode

    val gain = rootNode.stats.get
    assert(gain == InformationGainStats.invalidInformationGainStats)
  }
  //不要选择不满足每个节点要求的最小实例的分割
  test("do not choose split that does not satisfy min instance per node requirements") {
    // if a split does not satisfy min instances per node requirements,
    //如果一个分裂不满足每个节点的要求的最小实例
    // this split is invalid, even though the information gain of split is large.
    //这种分裂是无效的,即使分裂的信息增益是大的
    val arr = Array(
      LabeledPoint(0.0, Vectors.dense(0.0, 1.0)),
      LabeledPoint(1.0, Vectors.dense(1.0, 1.0)),
      LabeledPoint(0.0, Vectors.dense(0.0, 0.0)),
      LabeledPoint(0.0, Vectors.dense(0.0, 0.0)))

    val rdd = sc.parallelize(arr)
    /**
      1.训练数据集 
      2.目标类别个数，即结果有几种选择 
      3.Map中的键值分别对应Vector下标和该下标对应类别特征的取值情况，
                 空表示所有特征都是数值型（为了方便，示例中直接取空，实际当中并不能这么使用） 
      4.不纯性(impurity)度量：gini或者entropy，不纯度用来衡量一个规则的好坏，
                 好的规则可以将数据划分为等值的两部分，坏规则则相反 
      5.决策树的最大深度，越深的决策树越有可能产生过度拟合的问题 
      6.决策树的最大桶数，每层使用的决策规则的个数，越多就可能精确，花费的时候也就越多,
                   最小的桶数应该不小于类别特征中最大的选择个数
    **/
    val strategy = new Strategy(algo = Classification, impurity = Gini,
      maxBins = 2, maxDepth = 2, categoricalFeaturesInfo = Map(0 -> 2, 1-> 2),
      numClasses = 2, minInstancesPerNode = 2)//切分后每个子节点至少包含的样本实例数,否则停止切分,于终止迭代计算

    val rootNode = DecisionTree.train(rdd, strategy).topNode

    val split = rootNode.split.get
    val gain = rootNode.stats.get
    assert(split.feature == 1)
    assert(gain != InformationGainStats.invalidInformationGainStats)
  }
  //分隔必须满足最小信息增益的要求
  test("split must satisfy min info gain requirements") {
    val arr = Array(
    //LabeledPoint标记点是局部向量,向量可以是密集型或者稀疏型,每个向量会关联了一个标签(label)
      LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 0.0)))),
      LabeledPoint(1.0, Vectors.sparse(2, Seq((1, 1.0)))),
      LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 1.0)))))

    val input = sc.parallelize(arr)//树的最大深度,为了防止过拟合,设定划分的终止条件
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 2,
      numClasses = 2, minInfoGain = 1.0)

    val model = DecisionTree.train(input, strategy)
    assert(model.topNode.isLeaf)
    assert(model.topNode.predict.predict == 0.0)
    val predicts = input.map(p => model.predict(p.features)).collect()
    predicts.foreach { predict =>
      assert(predict == 0.0)
    }

    // test when no valid split can be found
    //测试时,没有有效的分裂可以被发现
    val rootNode = model.topNode

    val gain = rootNode.stats.get
    assert(gain == InformationGainStats.invalidInformationGainStats)
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests of model save/load 模型保存/加载测试
  /////////////////////////////////////////////////////////////////////////////

  test("Node.subtreeIterator") {//子树迭代器
    val model = DecisionTreeSuite.createModel(Classification)
    val nodeIds = model.topNode.subtreeIterator.map(_.id).toArray.sorted
    assert(nodeIds === DecisionTreeSuite.createdModelNodeIds)
  }

  test("model save/load") {//模型保存/加载
    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString

    Array(Classification, Regression).foreach { algo =>
      val model = DecisionTreeSuite.createModel(algo)
      // Save model, load it back, and compare.
      try {
        model.save(sc, path)
        val sameModel = DecisionTreeModel.load(sc, path)
        DecisionTreeSuite.checkEqual(model, sameModel)
      } finally {
        Utils.deleteRecursively(tempDir)
      }
    }
  }
}

object DecisionTreeSuite extends SparkFunSuite {
/**
 * 验证分类器
 */
  def validateClassifier(
      model: DecisionTreeModel,
      input: Seq[LabeledPoint],
      requiredAccuracy: Double) {
    val predictions = input.map(x => model.predict(x.features))
    val numOffPredictions = predictions.zip(input).count { case (prediction, expected) =>
      prediction != expected.label
    }
    val accuracy = (input.length - numOffPredictions).toDouble / input.length
    assert(accuracy >= requiredAccuracy,
      s"validateClassifier calculated accuracy $accuracy but required $requiredAccuracy.")
  }
/**
 * 验证回归
 */
  def validateRegressor(
      model: DecisionTreeModel,
      input: Seq[LabeledPoint],
      requiredMSE: Double) {
    val predictions = input.map(x => model.predict(x.features))
    val squaredError = predictions.zip(input).map { case (prediction, expected) =>
      val err = prediction - expected.label
      err * err
    }.sum
    val mse = squaredError / input.length
    assert(mse <= requiredMSE, s"validateRegressor calculated MSE $mse but required $requiredMSE.")
  }
/**
 *生成有序标签数据点
 */
  def generateOrderedLabeledPointsWithLabel0(): Array[LabeledPoint] = {
    val arr = new Array[LabeledPoint](1000)
    for (i <- 0 until 1000) {
      val lp = new LabeledPoint(0.0, Vectors.dense(i.toDouble, 1000.0 - i))
      arr(i) = lp
    }
    arr
  }

  def generateOrderedLabeledPointsWithLabel1(): Array[LabeledPoint] = {
    val arr = new Array[LabeledPoint](1000)
    for (i <- 0 until 1000) {
      val lp = new LabeledPoint(1.0, Vectors.dense(i.toDouble, 999.0 - i))
      arr(i) = lp
    }
    arr
  }

  def generateOrderedLabeledPoints(): Array[LabeledPoint] = {
    val arr = new Array[LabeledPoint](1000)
    for (i <- 0 until 1000) {
      val label = if (i < 100) {
        0.0
      } else if (i < 500) {
        1.0
      } else if (i < 900) {
        0.0
      } else {
        1.0
      }
      arr(i) = new LabeledPoint(label, Vectors.dense(i.toDouble, 1000.0 - i))
    }
    arr
  }
/**
 * 生成二分类数据
 * [(1.0,[0.0,1.0]), (1.0,[0.0,1.0]), (1.0,[0.0,1.0]), (1.0,[0.0,1.0]), (1.0,[0.0,1.0])]
 */
  def generateCategoricalDataPoints(): Array[LabeledPoint] = {
    val arr = new Array[LabeledPoint](1000)
    for (i <- 0 until 1000) {
      if (i < 600) {
      //LabeledPoint标记点是局部向量,向量可以是密集型或者稀疏型,每个向量会关联了一个标签(label)
        arr(i) = new LabeledPoint(1.0, Vectors.dense(0.0, 1.0))
      } else {
        arr(i) = new LabeledPoint(0.0, Vectors.dense(1.0, 0.0))
      }
    }
    //println(">>>>"+arr.toList)
    arr
  }

  def generateCategoricalDataPointsAsJavaList(): java.util.List[LabeledPoint] = {
    generateCategoricalDataPoints().toList.asJava
  }
/**
 * 生成多类分类数据
 * [(1.0,[0.0,1.0]), (2.0,[0.0,1.0]), (1.0,[0.0,1.0]), (2.0,[0.0,1.0]), (1.0,[0.0,1.0])]
 */
  def generateCategoricalDataPointsForMulticlass(): Array[LabeledPoint] = {
    val arr = new Array[LabeledPoint](3000)
    for (i <- 0 until 3000) {
      if (i < 1000) {
      //LabeledPoint标记点是局部向量,向量可以是密集型或者稀疏型,每个向量会关联了一个标签(label)
        arr(i) = new LabeledPoint(2.0, Vectors.dense(2.0, 2.0))
      } else if (i < 2000) {
        arr(i) = new LabeledPoint(1.0, Vectors.dense(1.0, 2.0))
      } else {
        arr(i) = new LabeledPoint(2.0, Vectors.dense(2.0, 2.0))
      }
    }
    arr
  }
/**
 * 生成多类连续数据
 * [(1.0,[0.0,1.0]), (2.0,[0.0,1.0]), (1.0,[0.0,1.0]), (2.0,[0.0,1.0]), (1.0,[0.0,1.0])]
 */
  def generateContinuousDataPointsForMulticlass(): Array[LabeledPoint] = {
    val arr = new Array[LabeledPoint](3000)
    for (i <- 0 until 3000) {
      if (i < 2000) {
        arr(i) = new LabeledPoint(2.0, Vectors.dense(2.0, i))
      } else {
        arr(i) = new LabeledPoint(1.0, Vectors.dense(2.0, i))
      }
    }
    arr
  }

  def generateCategoricalDataPointsForMulticlassForOrderedFeatures():
    Array[LabeledPoint] = {
    val arr = new Array[LabeledPoint](3000)
    for (i <- 0 until 3000) {
      if (i < 1000) {
      //LabeledPoint标记点是局部向量,向量可以是密集型或者稀疏型,每个向量会关联了一个标签(label)
        arr(i) = new LabeledPoint(2.0, Vectors.dense(2.0, 2.0))
      } else if (i < 2000) {
        arr(i) = new LabeledPoint(1.0, Vectors.dense(1.0, 2.0))
      } else {
        arr(i) = new LabeledPoint(1.0, Vectors.dense(2.0, 2.0))
      }
    }
    arr
  }

  /** 
   *  Create a leaf node with the given node ID 
   *  用给定的节点标识创建一个叶节点
   *  */
  private def createLeafNode(id: Int): Node = {
    Node(nodeIndex = id, new Predict(0.0, 1.0), impurity = 0.5, isLeaf = true)
  }

  /**
   * Create an internal node with the given node ID and feature type.
   * 创建一个给定节点标识和特征类型的内部节点
   * Note: This does NOT set the child nodes.这不设置子节点
   */
  private def createInternalNode(id: Int, featureType: FeatureType): Node = {
    val node = Node(nodeIndex = id, new Predict(0.0, 1.0), impurity = 0.5, isLeaf = false)
    featureType match {
      case Continuous =>
        node.split = Some(new Split(feature = 0, threshold = 0.5, Continuous,
          categories = List.empty[Double]))
      case Categorical =>
        node.split = Some(new Split(feature = 1, threshold = 0.0, Categorical,
          categories = List(0.0, 1.0)))
    }
    // TODO: The information gain stats should be consistent with info in children: SPARK-7131
    //信息增益统计应与子类的信息相一致
    node.stats = Some(new InformationGainStats(gain = 0.1, impurity = 0.2,
      leftImpurity = 0.3, rightImpurity = 0.4, new Predict(1.0, 0.4), new Predict(0.0, 0.6)))
    node
  }

  /**
   * Create a tree model.  创建树模型
   * This is deterministic and contains a variety of node and feature types.
   * 这是确定性的,包含了各种节点和特征类型
   * TODO: Update to be a correct tree (with matching probabilities, impurities, etc.): SPARK-7131
   */
  private[spark] def createModel(algo: Algo): DecisionTreeModel = {
    val topNode = createInternalNode(id = 1, Continuous)
    val (node2, node3) = (createLeafNode(id = 2), createInternalNode(id = 3, Categorical))
    val (node6, node7) = (createLeafNode(id = 6), createLeafNode(id = 7))
    topNode.leftNode = Some(node2)
    topNode.rightNode = Some(node3)
    node3.leftNode = Some(node6)
    node3.rightNode = Some(node7)
    new DecisionTreeModel(topNode, algo)
  }

  /** 
   *  Sorted Node IDs matching the model returned by [[createModel()]]
   *  排序的节点标识匹配的模型返回 
   *  */
  private val createdModelNodeIds = Array(1, 2, 3, 6, 7)

  /**
   * Check if the two trees are exactly the same.
   * 检查两棵树是否完全相同
   * Note: I hesitate to override Node.equals since it could cause problems if users
   *       make mistakes such as creating loops of Nodes.
   * If the trees are not equal, this prints the two trees and throws an exception.
   * 如果树不相等,则打印两个树并抛出一个异常
   */
  private[mllib] def checkEqual(a: DecisionTreeModel, b: DecisionTreeModel): Unit = {
    try {
      assert(a.algo === b.algo)
      checkEqual(a.topNode, b.topNode)
    } catch {
      case ex: Exception =>
        throw new AssertionError("checkEqual failed since the two trees were not identical.\n" +
          "TREE A:\n" + a.toDebugString + "\n" +
          "TREE B:\n" + b.toDebugString + "\n", ex)
    }
  }

  /**
   * Return true iff the two nodes and their descendents are exactly the same.
   * 返回true,当两节点和他们的后代是完全相同的
   * Note: I hesitate to override Node.equals since it could cause problems if users
   *       make mistakes such as creating loops of Nodes.
   */
  private def checkEqual(a: Node, b: Node): Unit = {
    assert(a.id === b.id)
    assert(a.predict === b.predict)
    assert(a.impurity === b.impurity)
    assert(a.isLeaf === b.isLeaf)
    assert(a.split === b.split)
    (a.stats, b.stats) match {
      // TODO: Check other fields besides the infomation gain.
      //检查除了信息增益等领域
      case (Some(aStats), Some(bStats)) => assert(aStats.gain === bStats.gain)
      case (None, None) =>
      case _ => throw new AssertionError(
          s"Only one instance has stats defined. (a.stats: ${a.stats}, b.stats: ${b.stats})")
    }
    (a.leftNode, b.leftNode) match {
      case (Some(aNode), Some(bNode)) => checkEqual(aNode, bNode)
      case (None, None) =>
      case _ => throw new AssertionError("Only one instance has leftNode defined. " +
        s"(a.leftNode: ${a.leftNode}, b.leftNode: ${b.leftNode})")
    }
    (a.rightNode, b.rightNode) match {
      case (Some(aNode: Node), Some(bNode: Node)) => checkEqual(aNode, bNode)
      case (None, None) =>
      case _ => throw new AssertionError("Only one instance has rightNode defined. " +
        s"(a.rightNode: ${a.rightNode}, b.rightNode: ${b.rightNode})")
    }
  }
}
