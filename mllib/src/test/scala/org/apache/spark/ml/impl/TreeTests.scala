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

package org.apache.spark.ml.impl

import scala.collection.JavaConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.ml.attribute.{AttributeGroup, NominalAttribute, NumericAttribute}
import org.apache.spark.ml.tree._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, DataFrame}


private[ml] object TreeTests extends SparkFunSuite {

  /**
   * Convert the given data to a DataFrame, and set the features and label metadata.
   * 把给出的数据转换到一个DataFrame数据集,并设置特征和标签的元数据
   * @param data  Dataset.  Categorical features and labels must already have 0-based indices.
   * 												分类特征和标签必须已经有0指标
   *              This must be non-empty.
   * @param categoricalFeatures  Map: categorical feature index -> number of distinct values
   * 														指定离散特征,是一个map,featureId->K,其中K表示特征值可能的情况(0, 1, …, K-1)
   * @param numClasses  Number of classes label can take.  If 0, mark as continuous.
   * 										可采取的类的数量,如果0,连续
   * @return DataFrame with metadata
   */
  def setMetadata(
      data: RDD[LabeledPoint],
      categoricalFeatures: Map[Int, Int],
      numClasses: Int): DataFrame = {
    val sqlContext = new SQLContext(data.sparkContext)
    import sqlContext.implicits._
    val df = data.toDF()
    /**
    +-----+-------------+
    |label|     features|
    +-----+-------------+
    |  0.0|[0.0,2.0,3.0]|
    |  1.0|[0.0,3.0,1.0]|
    |  0.0|[0.0,2.0,2.0]|
    |  1.0|[0.0,3.0,9.0]|
    |  0.0|[0.0,2.0,6.0]|
    +-----+-------------+*/
    df.show(5)
    val numFeatures = data.first().features.size
    //获得特征数3
    val featuresAttributes = Range(0, numFeatures).map { feature =>
    // println(">>>>"+feature)
      // Map(0 -> 1)判断key是否包含feature
      if (categoricalFeatures.contains(feature)) {
        //创建可更改属性的副本
        NominalAttribute.defaultAttr.withIndex(feature).withNumValues(categoricalFeatures(feature))
      } else {
        NumericAttribute.defaultAttr.withIndex(feature)
      }
    }.toArray
    val featuresMetadata = new AttributeGroup("features", featuresAttributes).toMetadata()
    //println("===="+featuresMetadata.toString())
    val labelAttribute = if (numClasses == 0) {//numClasses 分类数
      NumericAttribute.defaultAttr.withName("label")
    } else {
      NominalAttribute.defaultAttr.withName("label").withNumValues(numClasses)
    }
    val labelMetadata = labelAttribute.toMetadata()
    /**
      +-------------+-----+
      |     features|label|
      +-------------+-----+
      |[0.0,2.0,3.0]|  0.0|
      |[0.0,3.0,1.0]|  1.0|
      |[0.0,2.0,2.0]|  0.0|
      |[0.0,3.0,9.0]|  1.0|
      |[0.0,2.0,6.0]|  0.0|
      +-------------+-----+*/
    df.select(df("features").as("features", featuresMetadata),
      df("label").as("label", labelMetadata)).show(5)
    df.select(df("features").as("features", featuresMetadata),
      df("label").as("label", labelMetadata))
      
  }

  /** Java-friendly version of [[setMetadata()]] */
  def setMetadata(
      data: JavaRDD[LabeledPoint],
      categoricalFeatures: java.util.Map[java.lang.Integer, java.lang.Integer],
      numClasses: Int): DataFrame = {
    setMetadata(data.rdd, categoricalFeatures.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap,
      numClasses)
  }

  /**
   * Check if the two trees are exactly the same.
   * 检查两棵树是否完全相同
   * Note: I hesitate to override Node.equals since it could cause problems if users
   *       make mistakes such as creating loops of Nodes.
   * If the trees are not equal, this prints the two trees and throws an exception.
   * 如果树不相等,打印两个树并抛出异常.
   */
  def checkEqual(a: DecisionTreeModel, b: DecisionTreeModel): Unit = {
    try {
      checkEqual(a.rootNode, b.rootNode)
    } catch {
      case ex: Exception =>
        throw new AssertionError("checkEqual failed since the two trees were not identical.\n" +
          "TREE A:\n" + a.toDebugString + "\n" +
          "TREE B:\n" + b.toDebugString + "\n", ex)
    }
  }

  /**
   * Return true iff the two nodes and their descendants are exactly the same.
   * 返回true,当且仅当两节点和他们的后代是完全相同
   * Note: I hesitate to override Node.equals since it could cause problems if users
   *       make mistakes such as creating loops of Nodes.
   */
  private def checkEqual(a: Node, b: Node): Unit = {
    //println(a.prediction+"\t"+a.impurity+"\t"+a.leftSide)
    //判断预测相同
    assert(a.prediction === b.prediction)
    //判断不纯度相同
    assert(a.impurity === b.impurity)
    (a, b) match {
      case (aye: InternalNode, bee: InternalNode) =>
       // println("split:"+aye.split+" leftChild:"+ aye.leftChild+" rightChild:"+aye.rightChild)
        //分隔相等
        assert(aye.split === bee.split)
        checkEqual(aye.leftChild, bee.leftChild)
        checkEqual(aye.rightChild, bee.rightChild)
      case (aye: LeafNode, bee: LeafNode) => // do nothing
      case _ =>
        throw new AssertionError("Found mismatched nodes")
    }
  }

  /**
   * Check if the two models are exactly the same.
   * 检查两个模型是否完全相同
   * If the models are not equal, this throws an exception.
   * 如果模型不相等，则抛出一个异常
   */
  def checkEqual(a: TreeEnsembleModel, b: TreeEnsembleModel): Unit = {
    try {
      a.trees.zip(b.trees).foreach { case (treeA, treeB) =>
        TreeTests.checkEqual(treeA, treeB)
      }
      //权重相同
      assert(a.treeWeights === b.treeWeights)
    } catch {
      case ex: Exception => throw new AssertionError(
        "checkEqual failed since the two tree ensembles were not identical")
    }
  }

  /**
   * Helper method for constructing a tree for testing.
   * 用于构造测试树的辅助方法
   * Given left, right children, construct a parent node.
   * @param split  Split for parent node 父节点的拆分
   * @return  Parent node with children attached
   */
  def buildParentNode(left: Node, right: Node, split: Split): Node = {
    val leftImp = left.impurityStats
    val rightImp = right.impurityStats
    val parentImp = leftImp.copy.add(rightImp)
    val leftWeight = leftImp.count / parentImp.count.toDouble
    val rightWeight = rightImp.count / parentImp.count.toDouble
    val gain = parentImp.calculate() -
      (leftWeight * leftImp.calculate() + rightWeight * rightImp.calculate())
    val pred = parentImp.predict
    new InternalNode(pred, parentImp.calculate(), gain, left, right, split, parentImp)
  }
}
