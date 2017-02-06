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
 * 特征值不需要正则化,优化调整迭代次数
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
    val cateor=OldDecisionTreeSuite.generateCategoricalDataPoints()
    categoricalDataPointsRDD =sc.parallelize(cateor)
    val cateor1=OldDecisionTreeSuite.generateOrderedLabeledPointsWithLabel0()
    orderedLabeledPointsWithLabel0RDD =sc.parallelize(cateor1)
    val cateor2=OldDecisionTreeSuite.generateOrderedLabeledPointsWithLabel1()
    orderedLabeledPointsWithLabel1RDD =sc.parallelize(cateor2)
    val cateor3=OldDecisionTreeSuite.generateCategoricalDataPointsForMulticlass()
    categoricalDataPointsForMulticlassRDD =sc.parallelize(cateor3)
    val cateor4=OldDecisionTreeSuite.generateContinuousDataPointsForMulticlass()
    continuousDataPointsForMulticlassRDD =sc.parallelize(cateor4)
    val cateor5=OldDecisionTreeSuite.generateCategoricalDataPointsForMulticlassForOrderedFeatures()
    categoricalDataPointsForMulticlassForOrderedFeaturesRDD = sc.parallelize(cateor5)
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
  //有序特征的二元分类方法
  test("Binary classification stump with ordered categorical features") {
    val dt = new DecisionTreeClassifier()
      .setImpurity("gini")//计算信息增益的准则
      .setMaxDepth(2)//树的最大深度,为了防止过拟合,设定划分的终止条件
      .setMaxBins(100)//maxBins最大分箱数,当某个特征的特征值为连续时,该参数意思是将连续的特征值离散化为多少份
     /**
           指明特征的类别对应值(类别),注意特征索引是从0开始的,0和4表示第1和第5个特征
     Map(0 -> 2,4->10)表示特征0有两个特征值(0和1),特征4有10个特征值{0,1,2,3,…,9}             
     **/
    val categoricalFeatures = Map(0 -> 3, 1-> 3)
    val numClasses = 2 //如果是分类树,指定有多少种类别
   /**
    categoricalDataPointsRDD
    +-----+---------+    
    |label| features|
    +-----+---------+
    |  1.0|[0.0,1.0]|
    |  1.0|[0.0,1.0]|
    |  0.0|[1.0,0.0]|
    |  0.0|[1.0,0.0]|
    +-----+---------+**/
    compareAPIs(categoricalDataPointsRDD, dt, categoricalFeatures, numClasses)
  }  
  //固定标签0.1熵的二进制分类的树,Gini
  test("Binary classification stump with fixed labels 0,1 for Entropy,Gini") {
    val dt = new DecisionTreeClassifier()
      .setMaxDepth(3)//树的最大深度,为了防止过拟合,设定划分的终止条件
      .setMaxBins(100)//maxBins最大分箱数,当某个特征的特征值为连续时,该参数意思是将连续的特征值离散化为多少份
    val numClasses = 2 //如果是分类树,指定有多少种类别
    /**
     * orderedLabeledPointsWithLabel0RDD,orderedLabeledPointsWithLabel1RDD
          +-----+------------+	+-----+-----------+
          |label|    features|	|label|   features|
          +-----+------------+	+-----+-----------+
          |  0.0| [0.0,1000.0]|	|  1.0|[0.0,999.0]|
          |  0.0| [1.0,999.0]|	|  1.0|[1.0,998.0]|
          |  0.0| [2.0,998.0]|	|  1.0|[2.0,997.0]|
          |  0.0| [3.0,997.0]|	|  1.0|[3.0,996.0]|
          |  0.0| [4.0,996.0]|	|  1.0|[4.0,995.0]|
          +-----+------------+	+-----+-----------+*/
    Array(orderedLabeledPointsWithLabel0RDD, orderedLabeledPointsWithLabel1RDD).foreach { rdd =>
      //访问支持不纯度
      DecisionTreeClassifier.supportedImpurities.foreach { impurity =>
        dt.setImpurity(impurity)//计算信息增益的准则
        compareAPIs(rdd, dt, categoricalFeatures = Map.empty[Int, Int], numClasses)
      }
    }
  }
  //多类分类的树和三元（无序）的分类特征
  test("Multiclass classification stump with 3-ary (unordered) categorical features") {
    /**categoricalDataPointsForMulticlassRDD
     *+-----+---------+
      |label| features|
      +-----+---------+
      |  2.0|[2.0,2.0]|
      |  1.0|[1.0,2.0]|
      |  2.0|[2.0,2.0]|
      |  1.0|[1.0,2.0]|
      |  2.0|[2.0,2.0]|
      +-----+---------+
     */
    val rdd = categoricalDataPointsForMulticlassRDD
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")//计算信息增益的准则
      .setMaxDepth(4)//树的最大深度,为了防止过拟合,设定划分的终止条件
    val numClasses = 3 //如果是分类树,指定有多少种类别
     /**
             指明特征的类别对应值(类别),注意特征索引是从0开始的,0和4表示第1和第5个特征
     Map(0 -> 2,4->10)表示特征0有两个特征值(0和1),特征4有10个特征值{0,1,2,3,…,9}             
     **/
    val categoricalFeatures = Map(0 -> 3, 1 -> 3)
    compareAPIs(rdd, dt, categoricalFeatures, numClasses)
  }
  //二分类1个连续的特征,检查off-by-1误差
  test("Binary classification stump with 1 continuous feature, to check off-by-1 error") {
    val arr = Array(
    //LabeledPoint标记点是局部向量,向量可以是密集型或者稀疏型,每个向量会关联了一个标签(label)
      LabeledPoint(0.0, Vectors.dense(0.0)),
      LabeledPoint(1.0, Vectors.dense(1.0)),
      LabeledPoint(1.0, Vectors.dense(2.0)),
      LabeledPoint(1.0, Vectors.dense(3.0)))
    val rdd = sc.parallelize(arr)
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")//计算信息增益的准则
      .setMaxDepth(4)//树的最大深度,为了防止过拟合,设定划分的终止条件
    val numClasses = 2 //如果是分类树,指定有多少种类别
    compareAPIs(rdd, dt, categoricalFeatures = Map.empty[Int, Int], numClasses)
  }
  //具有2个连续特征的二叉分类
  test("Binary classification stump with 2 continuous features") {
    val arr = Array(
    //LabeledPoint标记点是局部向量,向量可以是密集型或者稀疏型,每个向量会关联了一个标签(label)
      LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 0.0)))),
      LabeledPoint(1.0, Vectors.sparse(2, Seq((1, 1.0)))),
      LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 0.0)))),
      LabeledPoint(1.0, Vectors.sparse(2, Seq((1, 2.0)))))
    val rdd = sc.parallelize(arr)
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")//计算信息增益的准则
      .setMaxDepth(4)//树的最大深度,为了防止过拟合,设定划分的终止条件
    val numClasses = 2//如果是分类树,指定有多少种类别
    compareAPIs(rdd, dt, categoricalFeatures = Map.empty[Int, Int], numClasses)
  }
  //多类分类的树和无序的分类特征,只要有足够的垃圾箱
  test("Multiclass classification stump with unordered categorical features," +
    " with just enough bins") {
    //maxBins最大分箱数,当某个特征的特征值为连续时,该参数意思是将连续的特征值离散化为多少份
    val maxBins = 2 * (math.pow(2, 3 - 1).toInt - 1) // just enough bins to allow unordered features
    val rdd = categoricalDataPointsForMulticlassRDD
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")//树节点选择的不纯度的衡量指标
      .setMaxDepth(4)//树的最大深度,为了防止过拟合,设定划分的终止条件
      .setMaxBins(maxBins)//maxBins最大分箱数,当某个特征的特征值为连续时,该参数意思是将连续的特征值离散化为多少份
     /**
             指明特征的类别对应值(类别),注意特征索引是从0开始的,0和4表示第1和第5个特征
     Map(0 -> 2,4->10)表示特征0有两个特征值(0和1),特征4有10个特征值{0,1,2,3,…,9}             
     **/
    val categoricalFeatures = Map(0 -> 3, 1 -> 3)
    val numClasses = 3//如果是分类树,指定有多少种类别
    compareAPIs(rdd, dt, categoricalFeatures, numClasses)
  }
  //多类分类的树和连续性的特点
  test("Multiclass classification stump with continuous features") {
    val rdd = continuousDataPointsForMulticlassRDD
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")//计算信息增益的准则
      .setMaxDepth(4)//树的最大深度,为了防止过拟合,设定划分的终止条件
      //maxBins最大分箱数,当某个特征的特征值为连续时,该参数意思是将连续的特征值离散化为多少份
      .setMaxBins(100)
    val numClasses = 3//如果是分类树,指定有多少种类别
    compareAPIs(rdd, dt, categoricalFeatures = Map.empty[Int, Int], numClasses)
  }
  //多类分类的树和连续+无序分类特征
  test("Multiclass classification stump with continuous + unordered categorical features") {
    val rdd = continuousDataPointsForMulticlassRDD
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")//计算信息增益的准则
      .setMaxDepth(4)//树的最大深度,为了防止过拟合,设定划分的终止条件
      //maxBins最大分箱数,当某个特征的特征值为连续时,该参数意思是将连续的特征值离散化为多少份
      .setMaxBins(100)
     /**
             指明特征的类别对应值(类别),注意特征索引是从0开始的,0和4表示第1和第5个特征
     Map(0 -> 2,4->10)表示特征0有两个特征值(0和1),特征4有10个特征值{0,1,2,3,…,9}             
     **/
    val categoricalFeatures = Map(0 -> 3)
    val numClasses = 3//如果是分类树,指定有多少种类别
    compareAPIs(rdd, dt, categoricalFeatures, numClasses)
  }
  //多类分类的树和10进制(有序)的分类特征
  test("Multiclass classification stump with 10-ary (ordered) categorical features") {
    val rdd = categoricalDataPointsForMulticlassForOrderedFeaturesRDD
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")//计算信息增益的准则
      .setMaxDepth(4)//树的最大深度,为了防止过拟合,设定划分的终止条件
      //maxBins最大分箱数,当某个特征的特征值为连续时,该参数意思是将连续的特征值离散化为多少份
      .setMaxBins(100)
     /**
             指明特征的类别对应值(类别),注意特征索引是从0开始的,0和4表示第1和第5个特征
     Map(0 -> 2,4->10)表示特征0有两个特征值(0和1),特征4有10个特征值{0,1,2,3,…,9}             
     **/
    val categoricalFeatures = Map(0 -> 10, 1 -> 10)
    val numClasses = 3//随机森林需要训练的树的个数,默认值是 20,如果是分类树,指定有多少种类别
    compareAPIs(rdd, dt, categoricalFeatures, numClasses)
  }
  //多类分类树与10叉(有序)的分类特征,只要有足够的垃圾箱
  test("Multiclass classification tree with 10-ary (ordered) categorical features," +
      " with just enough bins") {
    val rdd = categoricalDataPointsForMulticlassForOrderedFeaturesRDD
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")//计算信息增益的准则
      .setMaxDepth(4)//树的最大深度,为了防止过拟合,设定划分的终止条件
      //最大分箱数,当某个特征的特征值为连续时,该参数意思是将连续的特征值离散化为多少份
      .setMaxBins(10)
    /**
             指明特征的类别对应值(类别),注意特征索引是从0开始的,0和4表示第1和第5个特征
     Map(0 -> 2,4->10)表示特征0有两个特征值(0和1),特征4有10个特征值{0,1,2,3,…,9}             
     **/
    val categoricalFeatures = Map(0 -> 10, 1 -> 10)
    val numClasses = 3//随机森林需要训练的树的个数,默认值是 20,如果是分类树,指定有多少种类别
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
      .setMaxDepth(2)//树的最大深度,为了防止过拟合,设定划分的终止条件
      .setMinInstancesPerNode(2)//切分后每个子节点至少包含的样本实例数,否则停止切分,于终止迭代计算
    val numClasses = 2//随机森林需要训练的树的个数,如果是分类树,指定有多少种类别
    compareAPIs(rdd, dt, categoricalFeatures = Map.empty[Int, Int], numClasses)
  }  
  //不要选择不满足每个节点要求的最小实例的分割
  test("do not choose split that does not satisfy min instance per node requirements") {
    // if a split does not satisfy min instances per node requirements,
    //如果一个分裂不满足每个节点的要求的最小实例
    // this split is invalid, even though the information gain of split is large.
    //这种分裂是无效的,即使分裂的信息增益是大的
    val arr = Array(
    //LabeledPoint标记点是局部向量,向量可以是密集型或者稀疏型,每个向量会关联了一个标签(label)
      LabeledPoint(0.0, Vectors.dense(0.0, 1.0)),
      LabeledPoint(1.0, Vectors.dense(1.0, 1.0)),
      LabeledPoint(0.0, Vectors.dense(0.0, 0.0)),
      LabeledPoint(0.0, Vectors.dense(0.0, 0.0)))
    val rdd = sc.parallelize(arr)
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")//计算信息增益的准则
      .setMaxBins(2)//最大分箱数,当某个特征的特征值为连续时,该参数意思是将连续的特征值离散化为多少份
      .setMaxDepth(2)//树的最大深度,为了防止过拟合,设定划分的终止条件
      .setMinInstancesPerNode(2)//切分后每个子节点至少包含的样本实例数,否则停止切分,于终止迭代计算
     /**
             指明特征的类别对应值(类别),注意特征索引是从0开始的,0和4表示第1和第5个特征
     Map(0 -> 2,4->10)表示特征0有两个特征值(0和1),特征4有10个特征值{0,1,2,3,…,9}             
     **/
    val categoricalFeatures = Map(0 -> 2, 1-> 2)
    val numClasses = 22//如果是分类树,指定有多少种类别,随机森林训练的树的个数
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
      .setMaxDepth(2)//树的最大深度,为了防止过拟合,设定划分的终止条件
      .setMinInfoGain(1.0)//分裂节点时所需最小信息增益
    val numClasses = 2//如果是分类树,指定有多少种类别,随机森林训练的树的个数
    compareAPIs(rdd, dt, categoricalFeatures = Map.empty[Int, Int], numClasses)
  }
  //预测原数据和预测概率
  test("predictRaw and predictProbability") {
    val rdd = continuousDataPointsForMulticlassRDD
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")//计算信息增益的准则
      .setMaxDepth(4)//树的最大深度,为了防止过拟合,设定划分的终止条件
      .setMaxBins(100)//最大分箱数,当某个特征的特征值为连续时,该参数意思是将连续的特征值离散化为多少份
   //指定离散特征,是一个map,featureId->K,其中K表示特征值可能的情况(0, 1, …, K-1)
    val categoricalFeatures = Map(0 -> 3)
    val numClasses = 3//如果是分类树,指定有多少种类别,随机森林训练的树的个数

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
    //setMetadata 把给出的数据转换到一个数据集,并设置特征和标签的元数据
    val df = TreeTests.setMetadata(data, Map(0 -> 1), 2)
    //树的最大深度,为了防止过拟合,设定划分的终止条件
    val dt = new DecisionTreeClassifier().setMaxDepth(3)
    //fit()方法将DataFrame转化为一个Transformer的算法
    val formdata= sqlContext.createDataFrame(data)
  
    val model = dt.fit(df) 
    /**
     *+-----+-------------+-------------+-----------+----------+
      |label|     features|rawPrediction|probability|prediction|
      +-----+-------------+-------------+-----------+----------+
      |  0.0|[0.0,2.0,3.0]|    [3.0,0.0]|  [1.0,0.0]|       0.0|
      |  1.0|[0.0,3.0,1.0]|    [0.0,2.0]|  [0.0,1.0]|       1.0|
      |  0.0|[0.0,2.0,2.0]|    [3.0,0.0]|  [1.0,0.0]|       0.0|
      |  1.0|[0.0,3.0,9.0]|    [0.0,2.0]|  [0.0,1.0]|       1.0|
      |  0.0|[0.0,2.0,6.0]|    [3.0,0.0]|  [1.0,0.0]|       0.0|
      +-----+-------------+-------------+-----------+----------+*/
    model.transform(formdata).show()
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
   * 将旧的树转换为新的格式,比较它们,如果失败他们不完全平等
   */
  def compareAPIs(
      data: RDD[LabeledPoint],
      dt: DecisionTreeClassifier,
      categoricalFeatures: Map[Int, Int],
      numClasses: Int): Unit = {
    //创建一个策略实例 
    val oldStrategy = dt.getOldStrategy(categoricalFeatures, numClasses)
    //
    val oldTree = OldDecisionTree.train(data, oldStrategy)
    //把给出的数据转换到一个DataFrame数据集
    val newData: DataFrame = TreeTests.setMetadata(data, categoricalFeatures, numClasses)
    //newData.show(5)
    //fit()方法将DataFrame转化为一个Transformer的算法
    val newTree = dt.fit(newData)
    // Use parent from newTree since this is not checked anyways.
    //使用newTree父树,因为这是没有检查反正
    val oldTreeAsNew = DecisionTreeClassificationModel.fromOld(
      oldTree, newTree.parent.asInstanceOf[DecisionTreeClassifier], categoricalFeatures)
    TreeTests.checkEqual(oldTreeAsNew, newTree)
  }
}
