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

// scalastyle:off println
package org.apache.spark.examples.ml

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.classification.{ClassificationModel, Classifier, ClassifierParams}
import org.apache.spark.ml.param.{IntParam, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.linalg.{BLAS, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
 * A simple example demonstrating how to write your own learning algorithm using Estimator,
 * 一个简单的例子演示如何用估计写自己的学习算法
 * Transformer, and other abstractions.转换和其他抽象
 * This mimics [[org.apache.spark.ml.classification.LogisticRegression]].
 * Run with
 * {{{
 * bin/run-example ml.DeveloperApiExample
 * }}}
 */
object DeveloperApiExample {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DeveloperApiExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // Prepare training data. 准备训练数据
    val training = sc.parallelize(Seq(
    //LabeledPoint标记点是局部向量,向量可以是密集型或者稀疏型,每个向量会关联了一个标签(label)
      LabeledPoint(1.0, Vectors.dense(0.0, 1.1, 0.1)),
      LabeledPoint(0.0, Vectors.dense(2.0, 1.0, -1.0)),
      LabeledPoint(0.0, Vectors.dense(2.0, 1.3, 1.0)),
      LabeledPoint(1.0, Vectors.dense(0.0, 1.2, -0.5))))

    // Create a LogisticRegression instance.  This instance is an Estimator.
      //创建一个逻辑回归实例,这个实例是一个估计量
    val lr = new MyLogisticRegression()
    // Print out the parameters, documentation, and any default values.
    //打印参数、文档和任何默认值
    println("MyLogisticRegression parameters:\n" + lr.explainParams() + "\n")

    // We may set parameters using setter methods.
    //我们可以使用setter方法设置参数
    lr.setMaxIter(10)

    // Learn a LogisticRegression model.  This uses the parameters stored in lr.
    //学习一个逻辑回归模型,这使用存储的参数
    //fit()方法将DataFrame转化为一个Transformer的算法
    val model = lr.fit(training.toDF())

    // Prepare test data. 准备测试数据
    val test = sc.parallelize(Seq(
    //LabeledPoint标记点是局部向量,向量可以是密集型或者稀疏型,每个向量会关联了一个标签(label)
      LabeledPoint(1.0, Vectors.dense(-1.0, 1.5, 1.3)),
      LabeledPoint(0.0, Vectors.dense(3.0, 2.0, -0.1)),
      LabeledPoint(1.0, Vectors.dense(0.0, 2.2, -1.5))))

    // Make predictions on test data. 对测试数据进行预测
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val sumPredictions: Double = model.transform(test.toDF())
      .select("features", "label", "prediction")
      .collect()
      .map { case Row(features: Vector, label: Double, prediction: Double) =>
        prediction
      }.sum
    //Scala 的参数断言 assert() 或 assume() 方法在对中间结果或私有方法的参数进行检验,不成功则抛出 AssertionError 异常
    assert(sumPredictions == 0.0,
      "MyLogisticRegression predicted something other than 0, even though all weights are 0!")

    sc.stop()
  }
}

/**
 * Example of defining a parameter trait for a user-defined type of [[Classifier]].
 * 定义一个用户定义类型的参数特征的例子
 * NOTE: This is private since it is an example.  In practice, you may not want it to be private.
 */
private trait MyLogisticRegressionParams extends ClassifierParams {

  /**
   * Param for max number of iterations
   * 最大数量的迭代参数
   * NOTE: The usual way to add a parameter to a model or algorithm is to include:
   * 通常的方法来添加一个模型或算法的参数是包括
   *   - val myParamName: ParamType
   *   - def getMyParamName
   *   - def setMyParamName
   * Here, we have a trait to be mixed in with the Estimator and Model (MyLogisticRegression
   * 有一个特质与估计和模型混合在一起
   * and MyLogisticRegressionModel).  We place the setter (setMaxIter) method in the Estimator
   * class since the maxIter parameter is only used during training (not in the Model).
   */
  val maxIter: IntParam = new IntParam(this, "maxIter", "max number of iterations")
  def getMaxIter: Int = $(maxIter)
}

/**
 * Example of defining a type of [[Classifier]].
 * 定义一个类型的例子[ [分类] ]
 * NOTE: This is private since it is an example.  In practice, you may not want it to be private.
 */
private class MyLogisticRegression(override val uid: String)
  extends Classifier[Vector, MyLogisticRegression, MyLogisticRegressionModel]
  with MyLogisticRegressionParams {

  def this() = this(Identifiable.randomUID("myLogReg"))

  setMaxIter(100) // Initialize

  // The parameter setter is in this class since it should return type MyLogisticRegression.
  //参数设置器是因为它应该返回类型mylogisticregression
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  // This method is used by fit()这种方法是用fit()
  override protected def train(dataset: DataFrame): MyLogisticRegressionModel = {
    // Extract columns from data using helper method.
    //使用辅助方法从数据中提取列
    val oldDataset = extractLabeledPoints(dataset)

    // Do learning to estimate the weight vector.
    //做学习估计权重向量
    val numFeatures = oldDataset.take(1)(0).features.size
    val weights = Vectors.zeros(numFeatures) // Learning would happen here. 学习会发生在这里

    // Create a model, and return it. 创建一个模型,并返回它
    new MyLogisticRegressionModel(uid, weights).setParent(this)
  }

  override def copy(extra: ParamMap): MyLogisticRegression = defaultCopy(extra)
}

/**
 * Example of defining a type of [[ClassificationModel]].
 * 定义一个类型的[分类模型]的例子
 * NOTE: This is private since it is an example.  In practice, you may not want it to be private.
 */
private class MyLogisticRegressionModel(
    override val uid: String,
    val weights: Vector)
  extends ClassificationModel[Vector, MyLogisticRegressionModel]
  with MyLogisticRegressionParams {

  // This uses the default implementation of transform(), which reads column "features" and outputs
  //这是用transform()默认实现,读取和输出列的“特点”
  // columns "prediction" and "rawPrediction."
  //列“预测”和“rawprediction。”

  // This uses the default implementation of predict(), which chooses the label corresponding to
  //这是用predict()默认实现
  // the maximum value returned by [[predictRaw()]].
  //选择标签对应最大值返回的predictraw() 

  /**
   * Raw prediction for each possible label. 对每个可能的标签的原始预测
   * The meaning of a "raw" prediction may vary between algorithms, but it intuitively gives
   * 原始”预测的含义可能会有所不同的算法
   * a measure of confidence in each possible label (where larger = more confident).
   * 但它直观地给出了一个测量每个可能的标签信心 rawPredictionCol 原始的算法预测结果的存储列的名称
   * This internal method is used to implement [[transform()]] and output [[rawPredictionCol]].
   *
   * @return  vector where element i is the raw prediction for label i.
   *          This raw prediction may be any real number, where a larger value indicates greater
   *          confidence for that label.
   */
  override protected def predictRaw(features: Vector): Vector = {
    val margin = BLAS.dot(features, weights)
    // There are 2 classes (binary classification), so we return a length-2 vector,
    // where index i corresponds to class i (i = 0, 1).
    Vectors.dense(-margin, margin)
  }

  /** 
   *  Number of classes the label can take.  2 indicates binary classification.
   *  标签可以采取的类的数量, 2表示二进制分类
   *  */
  override val numClasses: Int = 2

  /**
   * Create a copy of the model.创建模型的副本
   * The copy is shallow, except for the embedded paramMap, which gets a deep copy.
   * 复制是浅,除了嵌入式parammap,得到深拷贝
   * This is used for the default implementation of [[transform()]].
   */
  override def copy(extra: ParamMap): MyLogisticRegressionModel = {
    copyValues(new MyLogisticRegressionModel(uid, weights), extra).setParent(parent)
  }
}
// scalastyle:on println
