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
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
// $example off$
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SQLContext, DataFrame}

/**
 * 梯度提升决策树:综合多个决策树,消除噪声,避免过拟合
 * GBT的训练是每次训练一颗树,然后利用这颗树对每个实例进行预测,通过一个损失函数,计算损失函数的负梯度值作为残差,
 * 利用这个残差更新样本实例的label,然后再次训练一颗树去拟合残差,如此进行迭代,直到满足模型参数需求。
 * GBT只适用于二分类和回归,不支持多分类,在预测的时候,不像随机森林那样求平均值,GBT是将所有树的预测值相加求和。
 */
object GradientBoostedTreeClassifierExample {
  def main(args: Array[String]): Unit = {
   val conf = new SparkConf().setAppName("GradientBoostedTreeClassifierExample").setMaster("local[4]")
    val sc = new SparkContext(conf)
  
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // $example on$
    // Load and parse the data file, converting it to a DataFrame.
    /**
 *  libSVM的数据格式
 *  <label> <index1>:<value1> <index2>:<value2> ...
 *  其中<label>是训练数据集的目标值,对于分类,它是标识某类的整数(支持多个类);对于回归,是任意实数
 *  <index>是以1开始的整数,可以是不连续
 *  <value>为实数,也就是我们常说的自变量
 */
    import org.apache.spark.mllib.util.MLUtils
    val dataSVM=MLUtils.loadLibSVMFile(sc, "../data/mllib/sample_libsvm_data.txt")
    val data=sqlContext.createDataFrame(dataSVM)
    //val data = sqlContext.read.format("libsvm").load("../data/mllib/sample_libsvm_data.txt")
  

    // Index labels, adding metadata to the label column.
    // 索引标签,将元数据添加到标签列.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)//fit()方法将DataFrame转化为一个Transformer的算法
    // Automatically identify categorical features, and index them.
      //自动识别分类特征,并对它们进行索引,
    // Set maxCategories so features with > 4 distinct values are treated as continuous.
     //VectorIndexer是对数据集特征向量中的类别(离散值)特征进行编号
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)//最大类别数为5,(即某一列)中多于4个取值视为连续值,不给予转换
      .fit(data)//fit()方法将DataFrame转化为一个Transformer的算法

    // Split the data into training and test sets (30% held out for testing).
    //将数据分成训练和测试集(30%进行测试)
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    // Train a GBT model.
    //训练GBT分类模型
    val gbt = new GBTClassifier()
      .setLabelCol("indexedLabel")
       //训练数据集DataFrame中存储特征数据的列名
      .setFeaturesCol("indexedFeatures")
      .setMaxIter(10)

    // Convert indexed labels back to original labels.
    //转换索引标签回到原来的标签
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    // Chain indexers and GBT in a Pipeline.
     //PipeLine:将多个DataFrame和Estimator算法串成一个特定的ML Wolkflow
     //一个 Pipeline在结构上会包含一个或多个 PipelineStage,每一个 PipelineStage 都会完成一个任务
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, gbt, labelConverter))

    // Train model. This also runs the indexers.
    //fit()方法将DataFrame转化为一个Transformer的算法
    val model = pipeline.fit(trainingData)

    // Make predictions.
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val predictions = model.transform(testData)

    // Select example rows to display.
    /**
     *+--------------+-----+--------------------+
      |predictedLabel|label|            features|
      +--------------+-----+--------------------+
      |           0.0|  0.0|(692,[127,128,129...|
      |           1.0|  1.0|(692,[151,152,153...|
      |           1.0|  1.0|(692,[158,159,160...|
      |           0.0|  1.0|(692,[99,100,101,...|
      |           1.0|  1.0|(692,[97,98,99,12...|
      +--------------+-----+--------------------+
     */
    predictions.select("predictedLabel", "label", "features").show(5)

    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      //算法预测结果的存储列的名称, 默认是”prediction”
      .setPredictionCol("prediction")
      .setMetricName("precision")
     //预测准确率
    val accuracy = evaluator.evaluate(predictions)
    //Test Error = 0.03448275862068961
    println("Test Error = " + (1.0 - accuracy))
    //管道模型获得GBTClassificationModel模型
    val gbtModel = model.stages(2).asInstanceOf[GBTClassificationModel]
    println("Learned classification GBT model:\n" + gbtModel.toDebugString)
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println
