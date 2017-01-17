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
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.param.{ParamMap, ParamsSuite}
import org.apache.spark.ml.util.{MLTestingUtils, MetadataUtils}
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.classification.LogisticRegressionSuite._
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.Metadata
/**
 * one-vs-rest训练时依次把某个类别的样本归为一类,其他剩余的样本归为另一类
 */
class OneVsRestSuite extends SparkFunSuite with MLlibTestSparkContext {

  @transient var dataset: DataFrame = _
  @transient var rdd: RDD[LabeledPoint] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val nPoints = 1000

    // The following weights and xMean/xVariance are computed from iris dataset with lambda=0.2.
    // As a result, we are drawing samples from probability distribution of an actual model.
    val weights = Array(
      -0.57997, 0.912083, -0.371077, -0.819866, 2.688191,
      -0.16624, -0.84355, -0.048509, -0.301789, 4.170682)

    val xMean = Array(5.843, 3.057, 3.758, 1.199)
    val xVariance = Array(0.6856, 0.1899, 3.116, 0.581)
    rdd = sc.parallelize(generateMultinomialLogisticInput(
      weights, xMean, xVariance, true, nPoints, 42), 2)
    dataset = sqlContext.createDataFrame(rdd)
  }

  test("params") {//参数
    ParamsSuite.checkParams(new OneVsRest)
    val lrModel = new LogisticRegressionModel("lr", Vectors.dense(0.0), 0.0)
    val model = new OneVsRestModel("ovr", Metadata.empty, Array(lrModel))
    ParamsSuite.checkParams(model)
  }
/**
 * one-vs-rest训练时依次把某个类别的样本归为一类,其他剩余的样本归为另一类
 */
  test("one-vs-rest: default params") {//默认参数
    val numClasses = 3//numClasses 分类数
    val ova = new OneVsRest()
      .setClassifier(new LogisticRegression)
    assert(ova.getLabelCol === "label")//标签列名
    //预测结果列名
    assert(ova.getPredictionCol === "prediction")
    //fit()方法将DataFrame转化为一个Transformer的算法
    val ovaModel = ova.fit(dataset)

    // copied model must have the same parent.
    // 复制的模型必须有相同的父
    MLTestingUtils.checkCopy(ovaModel)
    //numClasses 分类数
    assert(ovaModel.models.size === numClasses)
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val transformedDataset = ovaModel.transform(dataset)

    // check for label metadata in prediction col
    //检查在预测Col标签元数据
    val predictionColSchema = transformedDataset.schema(ovaModel.getPredictionCol)
    //numClasses 分类数
    assert(MetadataUtils.getNumClasses(predictionColSchema) === Some(3))

    val ovaResults = transformedDataset
      .select("prediction", "label")
      .map(row => (row.getDouble(0), row.getDouble(1)))
     //numClasses 分类数,基于lbfgs优化损失函数,支持多分类
    val lr = new LogisticRegressionWithLBFGS().setIntercept(true).setNumClasses(numClasses)
    //正则化参数>=0
    lr.optimizer.setRegParam(0.1).setNumIterations(100)

    val model = lr.run(rdd)
    val results = model.predict(rdd.map(_.features)).zip(rdd.map(_.label))
    // determine the #confusion matrix in each class.
    // bound how much error we allow compared to multinomial logistic regression.
    val expectedMetrics = new MulticlassMetrics(results)
     //评估指标-多分类
    val ovaMetrics = new MulticlassMetrics(ovaResults)
    assert(expectedMetrics.confusionMatrix ~== ovaMetrics.confusionMatrix absTol 400)
  }
   //在训练运行过程中正确地传递标签元数据
  test("one-vs-rest: pass label metadata correctly during train") {
    val numClasses = 3//numClasses 分类数
    val ova = new OneVsRest()
    ova.setClassifier(new MockLogisticRegression)
	
    val labelMetadata = NominalAttribute.defaultAttr.withName("label").withNumValues(numClasses)
    val labelWithMetadata = dataset("label").as("label", labelMetadata.toMetadata())
    val features = dataset("features").as("features")
    val datasetWithLabelMetadata = dataset.select(labelWithMetadata, features)
    //fit()方法将DataFrame转化为一个Transformer的算法
    ova.fit(datasetWithLabelMetadata)
  }
  //确保标签的功能和预测列配置
  test("SPARK-8092: ensure label features and prediction cols are configurable") {
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexed")

    val indexedDataset = labelIndexer
      .fit(dataset)
      .transform(dataset)
      .drop("label")
      .withColumnRenamed("features", "f")

    val ova = new OneVsRest()
    ova.setClassifier(new LogisticRegression())
      .setLabelCol(labelIndexer.getOutputCol)//标签列名
      .setFeaturesCol("f")//训练数据集DataFrame中存储特征数据的列名
      //预测结果列名
      .setPredictionCol("p")

    val ovaModel = ova.fit(indexedDataset)
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val transformedDataset = ovaModel.transform(indexedDataset)
    val outputFields = transformedDataset.schema.fieldNames.toSet
    assert(outputFields.contains("p"))
  }

  test("SPARK-8049: OneVsRest shouldn't output temp columns") {//不应该输出临时列
    val logReg = new LogisticRegression()
      .setMaxIter(1)
    val ovr = new OneVsRest()
      .setClassifier(logReg)
      //fit()方法将DataFrame转化为一个Transformer的算法
      //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val output = ovr.fit(dataset).transform(dataset)
    assert(output.schema.fieldNames.toSet === Set("label", "features", "prediction"))
  }

  test("OneVsRest.copy and OneVsRestModel.copy") {
    val lr = new LogisticRegression()
      .setMaxIter(1)

    val ovr = new OneVsRest()
    withClue("copy with classifier unset should work") {
      ovr.copy(ParamMap(lr.maxIter -> 10))
    }
    ovr.setClassifier(lr)
    val ovr1 = ovr.copy(ParamMap(lr.maxIter -> 10))
    require(ovr.getClassifier.getOrDefault(lr.maxIter) === 1, "copy should have no side-effects")
    require(ovr1.getClassifier.getOrDefault(lr.maxIter) === 10,
      "copy should handle extra classifier params")
     //fit()方法将DataFrame转化为一个Transformer的算法
    val ovrModel = ovr1.fit(dataset).copy(ParamMap(lr.thresholds -> Array(0.9, 0.1)))
    ovrModel.models.foreach { case m: LogisticRegressionModel =>
      //在二进制分类中设置阈值,范围为[0，1],如果类标签1的估计概率>Threshold,则预测1,否则0
      require(m.getThreshold === 0.1, "copy should handle extra model params")
    }
  }
}

private class MockLogisticRegression(uid: String) extends LogisticRegression(uid) {

  def this() = this("mockLogReg")

  setMaxIter(1)

  override protected def train(dataset: DataFrame): LogisticRegressionModel = {
    val labelSchema = dataset.schema($(labelCol))
    // check for label attribute propagation.
    assert(MetadataUtils.getNumClasses(labelSchema).forall(_ == 2))
    super.train(dataset)
  }
}
