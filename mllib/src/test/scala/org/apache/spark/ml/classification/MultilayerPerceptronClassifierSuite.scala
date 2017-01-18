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
/**
 * 参考资料
 * Spark ML 在 1.5 版本后提供一个使用 BP(反向传播，Back Propagation) 算法训练的多层感知器实现，
 * BP 算法的学习目的是对网络的连接权值进行调整，使得调整后的网络对任一输入都能得到所期望的输出。
 * https://www.ibm.com/developerworks/cn/opensource/os-cn-spark-practice6/
 * 
 */
import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.classification.LogisticRegressionSuite._
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.sql.Row
/**
 * 多层感知器分类器 (MultilayerPerceptronClassifer),
 *  BP(反向传播，Back Propagation) 算法训练的多层感知器实现，BP 算法的学习目的是对网络的连接权值进行调整，
 *  使得调整后的网络对任一输入都能得到所期望的输出
 */
class MultilayerPerceptronClassifierSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("XOR function learning as binary classification problem with two outputs.") {
    val dataFrame = sqlContext.createDataFrame(Seq(
        (Vectors.dense(0.0, 0.0), 0.0),
        (Vectors.dense(0.0, 1.0), 1.0),
        (Vectors.dense(1.0, 0.0), 1.0),
        (Vectors.dense(1.0, 1.0), 0.0))
    ).toDF("features", "label")
    /**
     * setLayers
     * 这个参数是一个整型数组类型，第一个元素需要和特征向量的维度相等，最后一个元素需要训练数据的标签取值个数相等，
     * 如 2 分类问题就写 2。中间的元素有多少个就代表神经网络有多少个隐层，元素的取值代表了该层的神经元的个数。
     * 例如val layers = Array[Int](2,5,2)。
     */
    val layers = Array[Int](2, 5, 2)
    //多层感知器分类器 (MultilayerPerceptronClassifer)
    val trainer = new MultilayerPerceptronClassifier()
    /**
     * setLayers
     * 这个参数是一个整型数组类型，第一个元素需要和特征向量的维度相等，最后一个元素需要训练数据的标签取值个数相等，
     * 如 2 分类问题就写 2。中间的元素有多少个就代表神经网络有多少个隐层，元素的取值代表了该层的神经元的个数。
     * 例如val layers = Array[Int](2,5,2)。
     */
      .setLayers(layers)
    /**
     * setBlockSize
     * 该参数被前馈网络训练器用来将训练样本数据的每个分区都按照 blockSize 大小分成不同组，
     * 并且每个组内的每个样本都会被叠加成一个向量，以便于在各种优化算法间传递。
     * 该参数的推荐值是 10-1000，默认值是 128
     */
      .setBlockSize(1)//
      .setSeed(11L)//设置种子
      .setMaxIter(100)//优化算法求解的最大迭代次数,默认值是 100
      //predictionCol:预测结果的列名称。
      //tol:优化算法迭代求解过程的收敛阀值。默认值是 1e-4。不能为负数
      //labelCol：输入数据 DataFrame 中标签列的名称。
      //featuresCol:输入数据 DataFrame 中指标特征列的名称。
    val model = trainer.fit(dataFrame)//fit()方法将DataFrame转化为一个Transformer的算法
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val result = model.transform(dataFrame)
    val predictionAndLabels = result.select("prediction", "label").collect()
    predictionAndLabels.foreach { case Row(p: Double, l: Double) =>
      //p是预测值,l实际分类值
      assert(p == l)
    }
  }

  // TODO: implement a more rigorous test
  test("3 class classification with 2 hidden layers") {//3类分类有2个隐藏层
    val nPoints = 1000

    // The following weights are taken from OneVsRestSuite.scala
    // they represent 3-class iris dataset
    val weights = Array(
      -0.57997, 0.912083, -0.371077, -0.819866, 2.688191,
      -0.16624, -0.84355, -0.048509, -0.301789, 4.170682)
   //平均值
    val xMean = Array(5.843, 3.057, 3.758, 1.199)//数据维度4
    //平方
    val xVariance = Array(0.6856, 0.1899, 3.116, 0.581)//数据维度4
    val rdd = sc.parallelize(generateMultinomialLogisticInput(
      weights, xMean, xVariance, true, nPoints, 42), 2)
    val dataFrame = sqlContext.createDataFrame(rdd).toDF("label", "features")
    val numClasses = 3  //如果是分类树,指定有多少种类别,随机森林训练的树的个数
    val numIterations = 100
    val layers = Array[Int](4, 5, 4, numClasses) //如果是分类树,指定有多少种类别,随机森林训练的树的个数
    val trainer = new MultilayerPerceptronClassifier()
     /**
     * setLayers
     * 这个参数是一个整型数组类型，第一个元素需要和特征向量的维度相等，最后一个元素需要训练数据的标签取值个数相等，
     * 如 2 分类问题就写 2。中间的元素有多少个就代表神经网络有多少个隐层，元素的取值代表了该层的神经元的个数。
     * 例如val layers = Array[Int](100,6,5,2)。
     */
      .setLayers(layers)
     /**
     * setBlockSize
     * 该参数被前馈网络训练器用来将训练样本数据的每个分区都按照 blockSize 大小分成不同组，
     * 并且每个组内的每个样本都会被叠加成一个向量，以便于在各种优化算法间传递。
     * 该参数的推荐值是 10-1000，默认值是 128
     */
      .setBlockSize(1)      
      .setSeed(11L)
      //优化算法求解的最大迭代次数。默认值是 100。
      .setMaxIter(numIterations)
      //fit()方法将DataFrame转化为一个Transformer的算法
    val model = trainer.fit(dataFrame)
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val mlpPredictionAndLabels = model.transform(dataFrame).select("prediction", "label")
      .map { case Row(p: Double, l: Double) => (p, l) }
    // train multinomial logistic regression
    //基于lbfgs优化损失函数,支持多分类
    val lr = new LogisticRegressionWithLBFGS()
      .setIntercept(true)
       //如果是分类树,指定有多少种类别,随机森林训练的树的个数
      .setNumClasses(numClasses)
    lr.optimizer.setRegParam(0.0)//正则化参数>=0
      .setNumIterations(numIterations)
    val lrModel = lr.run(rdd)
    val lrPredictionAndLabels = lrModel.predict(rdd.map(_.features)).zip(rdd.map(_.label))
    // MLP's predictions should not differ a lot from LR's.
     //评估指标-多分类
    val lrMetrics = new MulticlassMetrics(lrPredictionAndLabels)
    val mlpMetrics = new MulticlassMetrics(mlpPredictionAndLabels)
    assert(mlpMetrics.confusionMatrix ~== lrMetrics.confusionMatrix absTol 100)
  }
}
