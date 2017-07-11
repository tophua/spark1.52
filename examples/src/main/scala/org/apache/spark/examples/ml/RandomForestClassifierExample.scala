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

// $example on$
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
// $example off$
import org.apache.spark.sql.Row
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SQLContext, DataFrame}
/**
 * 随机森林(Random Forests)其实就是多个决策树,每个决策树有一个权重,对未知数据进行预测时,
 * 会用多个决策树分别预测一个值,然后考虑树的权重,将这多个预测值综合起来,
 * 对于分类问题,采用多数表决,对于回归问题,直接求平均。
 * RandomForestClassifier 随机森林分类树
 */
object RandomForestClassifierExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RandomForestClassifierExample").setMaster("local[4]")
    val sc = new SparkContext(conf)
  
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // $example on$
    // Load and parse the data file, converting it to a DataFrame.
   // val data = sqlContext.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")
      import org.apache.spark.mllib.util.MLUtils
      val dataSVM=MLUtils.loadLibSVMFile(sc, "../data/mllib/sample_libsvm_data.txt")
      val data = sqlContext.createDataFrame(dataSVM)
    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    //训练之前我们使用了两种数据预处理方法来对特征进行转换
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)//fit()方法将DataFrame转化为一个Transformer的算法
    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4 distinct values are treated as continuous.
    //VectorIndexer是对数据集特征向量中的类别(离散值)特征进行编号
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)//最大类别数为4,(即某一列)中多于4个取值视为连续值
      .fit(data)//fit()方法将DataFrame转化为一个Transformer的算法

    // Split the data into training and test sets (30% held out for testing).
    //使用第一部分数据进行训练,剩下数据来测试
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    // Train a RandomForest model.
    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")//标签列名
      .setFeaturesCol("indexedFeatures")//特征列名
      .setNumTrees(10)//训练的树的数量

    // Convert indexed labels back to original labels.
    //转换索引标签回到原来的标签
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    // Chain indexers and forest in a Pipeline.
     //PipeLine:将多个DataFrame和Estimator算法串成一个特定的ML Wolkflow
     //一个 Pipeline在结构上会包含一个或多个 PipelineStage,每一个 PipelineStage 都会完成一个任务
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

    // Train model. This also runs the indexers.
    //fit()方法将DataFrame转化为一个Transformer的算法
    val model = pipeline.fit(trainingData)

    // Make predictions.
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val predictions = model.transform(testData)
    /**
      +-----+--------------------+------------+--------------------+-------------+-----------+----------+--------------+
      |label|            features|indexedLabel|     indexedFeatures|rawPrediction|probability|prediction|predictedLabel|
      +-----+--------------------+------------+--------------------+-------------+-----------+----------+--------------+
      |  0.0|(692,[127,128,129...|         1.0|(692,[127,128,129...|   [0.0,10.0]|  [0.0,1.0]|       1.0|           0.0|
      |  1.0|(692,[152,153,154...|         0.0|(692,[152,153,154...|   [10.0,0.0]|  [1.0,0.0]|       0.0|           1.0|
      |  0.0|(692,[154,155,156...|         1.0|(692,[154,155,156...|    [6.0,4.0]|  [0.6,0.4]|       0.0|           1.0|
      |  0.0|(692,[124,125,126...|         1.0|(692,[124,125,126...|   [0.0,10.0]|  [0.0,1.0]|       1.0|           0.0|
      |  1.0|(692,[124,125,126...|         0.0|(692,[124,125,126...|   [10.0,0.0]|  [1.0,0.0]|       0.0|           1.0|
      +-----+--------------------+------------+--------------------+-------------+-----------+----------+--------------+*/
    predictions.show(5)
    // Select example rows to display.
    /**
      +--------------+-----+--------------------+
      |predictedLabel|label|            features|
      +--------------+-----+--------------------+
      |           1.0|  1.0|(692,[158,159,160...|
      |           1.0|  1.0|(692,[124,125,126...|
      |           0.0|  0.0|(692,[129,130,131...|
      |           1.0|  1.0|(692,[158,159,160...|
      |           0.0|  1.0|(692,[99,100,101,...|
      +--------------+-----+--------------------+*/
    predictions.select("predictedLabel", "label", "features").show(5)

    // Select (prediction, true label) and compute test error.
    // 选择(预测,标签)计算测试错误
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")//标签列名
      //算法预测结果的存储列的名称, 默认是”prediction”
      .setPredictionCol("prediction")
      .setMetricName("precision")//准确率
    val accuracy = evaluator.evaluate(predictions)
    //Test Error = 0.025000000000000022
    println("Test Error = " + (1.0 - accuracy))

    val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
    //Learned classification forest model:
    println("Learned classification forest model:\n" + rfModel.toDebugString)
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println
