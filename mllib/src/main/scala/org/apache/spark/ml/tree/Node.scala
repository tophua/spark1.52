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

package org.apache.spark.ml.tree

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.tree.impurity.ImpurityCalculator
import org.apache.spark.mllib.tree.model.{InformationGainStats => OldInformationGainStats,
  Node => OldNode, Predict => OldPredict, ImpurityStats}

/**
 * :: DeveloperApi ::
 * Decision tree node interface.
  * 决策树节点接口
 */
@DeveloperApi
sealed abstract class Node extends Serializable {

  // TODO: Add aggregate stats (once available).  This will happen after we move the DecisionTree
  //       code into the new API and deprecate the old API.  SPARK-3727

  /** Prediction a leaf node makes, or which an internal node would make if it were a leaf node
    * 如果叶节点是叶节点,则预测叶节点,或者内部节点将进行预测 */
  def prediction: Double

  /** Impurity measure at this node (for training data)
    * 此节点的杂质度量(用于训练数据)*/
  def impurity: Double

  /**
   * Statistics aggregated from training data at this node, used to compute prediction, impurity,
   * and probabilities.
    * 从该节点的训练数据汇总的统计数据,用于计算预测,杂质和概率
   * For classification, the array of class counts must be normalized to a probability distribution.
    * 对于分类,必须将类计数数组标准化为概率分布
   */
  private[ml] def impurityStats: ImpurityCalculator

  /** Recursive prediction helper method 递归预测辅助方法*/
  private[ml] def predictImpl(features: Vector): LeafNode

  /**
   * Get the number of nodes in tree below this node, including leaf nodes.
    * 获取此节点下方树中的节点数,包括叶节点
   * E.g., if this is a leaf, returns 0.  If both children are leaves, returns 2.
   */
  private[tree] def numDescendants: Int

  /**
   * Recursive print function.递归打印功能
   * @param indentFactor  The number of spaces to add to each level of indentation.
   */
  private[tree] def subtreeToString(indentFactor: Int = 0): String

  /**
   * Get depth of tree from this node.从此节点获取树的深度
   * E.g.: Depth 0 means this is a leaf node.  Depth 1 means 1 internal and 2 leaf nodes.
   */
  private[tree] def subtreeDepth: Int

  /**
   * Create a copy of this node in the old Node format, recursively creating child nodes as needed.
    * 以旧节点格式创建此节点的副本,根据需要递归创建子节点
   * @param id  Node ID using old format IDs
   */
  private[ml] def toOld(id: Int): OldNode

  /**
   * Trace down the tree, and return the largest feature index used in any split.
    * 跟踪树,并返回任何拆分中使用的最大特征索引
   * @return  Max feature index used in a split, or -1 if there are no splits (single leaf node).
   */
  private[ml] def maxSplitFeatureIndex(): Int
}

private[ml] object Node {

  /**
   * Create a new Node from the old Node format, recursively creating child nodes as needed.
    * 从旧节点格式创建新节点,根据需要递归创建子节点
   */
  def fromOld(oldNode: OldNode, categoricalFeatures: Map[Int, Int]): Node = {
    if (oldNode.isLeaf) {
      // TODO: Once the implementation has been moved to this API, then include sufficient
      //       statistics here.
      new LeafNode(prediction = oldNode.predict.predict,
        impurity = oldNode.impurity, impurityStats = null)
    } else {
      val gain = if (oldNode.stats.nonEmpty) {
        oldNode.stats.get.gain
      } else {
        0.0
      }
      new InternalNode(prediction = oldNode.predict.predict, impurity = oldNode.impurity,
        gain = gain, leftChild = fromOld(oldNode.leftNode.get, categoricalFeatures),
        rightChild = fromOld(oldNode.rightNode.get, categoricalFeatures),
        split = Split.fromOld(oldNode.split.get, categoricalFeatures), impurityStats = null)
    }
  }
}

/**
 * :: DeveloperApi ::
 * Decision tree leaf node.决策树叶节点
 * @param prediction  Prediction this node makes
 * @param impurity  Impurity measure at this node (for training data)
 */
@DeveloperApi
final class LeafNode private[ml] (
    override val prediction: Double,
    override val impurity: Double,
    override private[ml] val impurityStats: ImpurityCalculator) extends Node {

  override def toString: String =
    s"LeafNode(prediction = $prediction, impurity = $impurity)"

  override private[ml] def predictImpl(features: Vector): LeafNode = this

  override private[tree] def numDescendants: Int = 0

  override private[tree] def subtreeToString(indentFactor: Int = 0): String = {
    val prefix: String = " " * indentFactor
    prefix + s"Predict: $prediction\n"
  }

  override private[tree] def subtreeDepth: Int = 0

  override private[ml] def toOld(id: Int): OldNode = {
    new OldNode(id, new OldPredict(prediction, prob = impurityStats.prob(prediction)),
      impurity, isLeaf = true, None, None, None, None)
  }

  override private[ml] def maxSplitFeatureIndex(): Int = -1
}

/**
 * :: DeveloperApi ::
 * Internal Decision Tree node.内部决策树节点
 * @param prediction  Prediction this node would make if it were a leaf node
 * @param impurity  Impurity measure at this node (for training data)
 * @param gain Information gain value.
 *             Values < 0 indicate missing values; this quirk will be removed with future updates.
 * @param leftChild  Left-hand child node
 * @param rightChild  Right-hand child node
 * @param split  Information about the test used to split to the left or right child.
 */
@DeveloperApi
final class InternalNode private[ml] (
    override val prediction: Double,
    override val impurity: Double,
    val gain: Double,
    val leftChild: Node,
    val rightChild: Node,
    val split: Split,
    override private[ml] val impurityStats: ImpurityCalculator) extends Node {

  override def toString: String = {
    s"InternalNode(prediction = $prediction, impurity = $impurity, split = $split)"
  }

  override private[ml] def predictImpl(features: Vector): LeafNode = {
    if (split.shouldGoLeft(features)) {
      leftChild.predictImpl(features)
    } else {
      rightChild.predictImpl(features)
    }
  }

  override private[tree] def numDescendants: Int = {
    2 + leftChild.numDescendants + rightChild.numDescendants
  }

  override private[tree] def subtreeToString(indentFactor: Int = 0): String = {
    val prefix: String = " " * indentFactor
    prefix + s"If (${InternalNode.splitToString(split, left = true)})\n" +
      leftChild.subtreeToString(indentFactor + 1) +
      prefix + s"Else (${InternalNode.splitToString(split, left = false)})\n" +
      rightChild.subtreeToString(indentFactor + 1)
  }

  override private[tree] def subtreeDepth: Int = {
    1 + math.max(leftChild.subtreeDepth, rightChild.subtreeDepth)
  }

  override private[ml] def toOld(id: Int): OldNode = {
    assert(id.toLong * 2 < Int.MaxValue, "Decision Tree could not be converted from new to old API"
      + " since the old API does not support deep trees.")
    new OldNode(id, new OldPredict(prediction, prob = impurityStats.prob(prediction)), impurity,
      isLeaf = false, Some(split.toOld), Some(leftChild.toOld(OldNode.leftChildIndex(id))),
      Some(rightChild.toOld(OldNode.rightChildIndex(id))),
      Some(new OldInformationGainStats(gain, impurity, leftChild.impurity, rightChild.impurity,
        new OldPredict(leftChild.prediction, prob = 0.0),
        new OldPredict(rightChild.prediction, prob = 0.0))))
  }

  override private[ml] def maxSplitFeatureIndex(): Int = {
    math.max(split.featureIndex,
      math.max(leftChild.maxSplitFeatureIndex(), rightChild.maxSplitFeatureIndex()))
  }
}

private object InternalNode {

  /**
   * Helper method for [[Node.subtreeToString()]].
   * @param split  Split to print
   * @param left  Indicates whether this is the part of the split going to the left,
   *              or that going to the right.
   */
  private def splitToString(split: Split, left: Boolean): String = {
    val featureStr = s"feature ${split.featureIndex}"
    split match {
      case contSplit: ContinuousSplit =>
        if (left) {
          s"$featureStr <= ${contSplit.threshold}"
        } else {
          s"$featureStr > ${contSplit.threshold}"
        }
      case catSplit: CategoricalSplit =>
        val categoriesStr = catSplit.leftCategories.mkString("{", ",", "}")
        if (left) {
          s"$featureStr in $categoriesStr"
        } else {
          s"$featureStr not in $categoriesStr"
        }
    }
  }
}

/**
 * Version of a node used in learning.  This uses vars so that we can modify nodes as we split the
 * tree by adding children, etc.
 * 学习中使用的节点版本,这使用变量,以便我们可以在通过添加子项等分割树时修改节点
  *
 * For now, we use node IDs.  These will be kept internal since we hope to remove node IDs
 * in the future, or at least change the indexing (so that we can support much deeper trees).
 *
 * This node can either be:
 *  - a leaf node, with leftChild, rightChild, split set to null, or
 *  - an internal node, with all values set
 *
 * @param id  We currently use the same indexing as the old implementation in
 *            [[org.apache.spark.mllib.tree.model.Node]], but this will change later.
 * @param isLeaf  Indicates whether this node will definitely be a leaf in the learned tree,
 *                so that we do not need to consider splitting it further.
 * @param stats  Impurity statistics for this node.
 */
private[tree] class LearningNode(
    var id: Int,
    var leftChild: Option[LearningNode],
    var rightChild: Option[LearningNode],
    var split: Option[Split],
    var isLeaf: Boolean,
    var stats: ImpurityStats) extends Serializable {

  /**
   * Convert this [[LearningNode]] to a regular [[Node]], and recurse on any children.
    * 将此[[LearningNode]]转换为常规[[Node]],并递归任何子项
   */
  def toNode: Node = {
    if (leftChild.nonEmpty) {
      assert(rightChild.nonEmpty && split.nonEmpty && stats != null,
        "Unknown error during Decision Tree learning.  Could not convert LearningNode to Node.")
      new InternalNode(stats.impurityCalculator.predict, stats.impurity, stats.gain,
        leftChild.get.toNode, rightChild.get.toNode, split.get, stats.impurityCalculator)
    } else {
      if (stats.valid) {
        new LeafNode(stats.impurityCalculator.predict, stats.impurity,
          stats.impurityCalculator)
      } else {
        // Here we want to keep same behavior with the old mllib.DecisionTreeModel
        new LeafNode(stats.impurityCalculator.predict, -1.0, stats.impurityCalculator)
      }

    }
  }

}

private[tree] object LearningNode {

  /** Create a node with some of its fields set.
    * 创建一个设置了一些字段的节点*/
  def apply(
      id: Int,
      isLeaf: Boolean,
      stats: ImpurityStats): LearningNode = {
    new LearningNode(id, None, None, None, false, stats)
  }

  /** Create an empty node with the given node index.  Values must be set later on.
    * 使用给定的节点索引创建一个空节点,必须稍后设置值 */
  def emptyNode(nodeIndex: Int): LearningNode = {
    new LearningNode(nodeIndex, None, None, None, false, null)
  }

  // The below indexing methods were copied from spark.mllib.tree.model.Node

  /**
   * Return the index of the left child of this node.
    * 返回此节点的左子节点的索引
   */
  def leftChildIndex(nodeIndex: Int): Int = nodeIndex << 1

  /**
   * Return the index of the right child of this node.
    * 返回此节点的右子节点的索引
   */
  def rightChildIndex(nodeIndex: Int): Int = (nodeIndex << 1) + 1

  /**
   * Get the parent index of the given node, or 0 if it is the root.
    * 获取给定节点的父索引,如果是根,则获取0
   */
  def parentIndex(nodeIndex: Int): Int = nodeIndex >> 1

  /**
   * Return the level of a tree which the given node is in.
    * 返回给定节点所在的树的级别
   */
  def indexToLevel(nodeIndex: Int): Int = if (nodeIndex == 0) {
    throw new IllegalArgumentException(s"0 is not a valid node index.")
  } else {
    java.lang.Integer.numberOfTrailingZeros(java.lang.Integer.highestOneBit(nodeIndex))
  }

  /**
   * Returns true if this is a left child.
    * 如果这是一个左子,则返回true
   * Note: Returns false for the root.
   */
  def isLeftChild(nodeIndex: Int): Boolean = nodeIndex > 1 && nodeIndex % 2 == 0

  /**
   * Return the maximum number of nodes which can be in the given level of the tree.
    * 返回可以在树的给定级别中的最大节点数
   * @param level  Level of tree (0 = root).
   */
  def maxNodesInLevel(level: Int): Int = 1 << level

  /**
   * Return the index of the first node in the given level.
    * 返回给定级别中第一个节点的索引
   * @param level  Level of tree (0 = root).
   */
  def startIndexInLevel(level: Int): Int = 1 << level

  /**
   * Traces down from a root node to get the node with the given node index.
    * 从根节点跟踪以获取具有给定节点索引的节点
   * This assumes the node exists.
   */
  def getNode(nodeIndex: Int, rootNode: LearningNode): LearningNode = {
    var tmpNode: LearningNode = rootNode
    var levelsToGo = indexToLevel(nodeIndex)
    while (levelsToGo > 0) {
      if ((nodeIndex & (1 << levelsToGo - 1)) == 0) {
        tmpNode = tmpNode.leftChild.asInstanceOf[LearningNode]
      } else {
        tmpNode = tmpNode.rightChild.asInstanceOf[LearningNode]
      }
      levelsToGo -= 1
    }
    tmpNode
  }

}
