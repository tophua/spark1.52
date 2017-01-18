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

package org.apache.spark.ml.tree.impl

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.impl.TreeTests
import org.apache.spark.ml.tree.{ContinuousSplit, DecisionTreeModel, LeafNode, Node}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.tree.impurity.GiniCalculator
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.util.collection.OpenHashMap

/**
 * 随机森林树:综合多个决策树,可以消除噪声,避免过拟合
 * Test suite for [[RandomForest]].
 */
class RandomForestSuite extends SparkFunSuite with MLlibTestSparkContext {

  import RandomForestSuite.mapToVec

  test("computeFeatureImportance, featureImportances") {//计算功能的重要性,功能的重要性
    /* Build tree for testing, with this structure:
     * 用于测试的生成树,有了这个结构:
          grandParent
      left2       parent
                left  right
     */
    //基尼计算器
    val leftImp = new GiniCalculator(Array(3.0, 2.0, 1.0))
    val left = new LeafNode(0.0, leftImp.calculate(), leftImp)

    val rightImp = new GiniCalculator(Array(1.0, 2.0, 5.0))
    val right = new LeafNode(2.0, rightImp.calculate(), rightImp)

    val parent = TreeTests.buildParentNode(left, right, new ContinuousSplit(0, 0.5))
    val parentImp = parent.impurityStats

    val left2Imp = new GiniCalculator(Array(1.0, 6.0, 1.0))
    val left2 = new LeafNode(0.0, left2Imp.calculate(), left2Imp)
    //重大父母
    val grandParent = TreeTests.buildParentNode(left2, parent, new ContinuousSplit(1, 1.0))
    val grandImp = grandParent.impurityStats

    // Test feature importance computed at different subtrees.
    //计算测试特征的重要性在不同的子树
    def testNode(node: Node, expected: Map[Int, Double]): Unit = {
      val map = new OpenHashMap[Int, Double]()
      RandomForest.computeFeatureImportance(node, map)
      assert(mapToVec(map.toMap) ~== mapToVec(expected) relTol 0.01)
    }

    // Leaf node
    //叶节点
    testNode(left, Map.empty[Int, Double])

    // Internal node with 2 leaf children
    //具有2个叶的儿童的内部节点
    val feature0importance = parentImp.calculate() * parentImp.count -
      (leftImp.calculate() * leftImp.count + rightImp.calculate() * rightImp.count)
    testNode(parent, Map(0 -> feature0importance))

    // Full tree
    //满树
    //重要特征1
    val feature1importance = grandImp.calculate() * grandImp.count -
      (left2Imp.calculate() * left2Imp.count + parentImp.calculate() * parentImp.count)
    testNode(grandParent, Map(0 -> feature0importance, 1 -> feature1importance))

    // Forest consisting of (full tree) + (internal node with 2 leafs)
    //林组成(全树)+(2内部节点的叶子)
    val trees = Array(parent, grandParent).map { root =>//numClasses 分类数
      new DecisionTreeClassificationModel(root, numClasses = 3).asInstanceOf[DecisionTreeModel]
    }
    //重要
    val importances: Vector = RandomForest.featureImportances(trees, 2)
    val tree2norm = feature0importance + feature1importance
    val expected = Vectors.dense((1.0 + feature0importance / tree2norm) / 2.0,
      (feature1importance / tree2norm) / 2.0)
    assert(importances ~== expected relTol 0.01)
  }

  test("normalizeMapValues") {//规范Map的值
    val map = new OpenHashMap[Int, Double]()
    map(0) = 1.0
    map(2) = 2.0
    RandomForest.normalizeMapValues(map)
    val expected = Map(0 -> 1.0 / 3.0, 2 -> 2.0 / 3.0)
    assert(mapToVec(map.toMap) ~== mapToVec(expected) relTol 0.01)
  }

}
/**
 * 随机森林树套件
 */
private object RandomForestSuite {

  def mapToVec(map: Map[Int, Double]): Vector = {
    val size = (map.keys.toSeq :+ 0).max + 1
    val (indices, values) = map.toSeq.sortBy(_._1).unzip
    Vectors.sparse(size, indices.toArray, values.toArray)
  }
}
