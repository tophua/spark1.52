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
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
// $example off$
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.mllib.util.MLUtils
object DecisionTreeClassificationExample {
  def main(args: Array[String]): Unit = {
  val conf = new SparkConf().setAppName("DecisionTreeClassificationExample").setMaster("local[4]")
    val sc = new SparkContext(conf)
  
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    
    // $example on$
    // Load the data stored in LIBSVM format as a DataFrame.
    /**
 *  libSVM的数据格式
 *  <label> <index1>:<value1> <index2>:<value2> ...
 *  其中<label>是训练数据集的目标值,对于分类,它是标识某类的整数(支持多个类);对于回归,是任意实数
 *  <index>是以1开始的整数,可以是不连续
 *  <value>为实数,也就是我们常说的自变量
 */
    val dataSVM=MLUtils.loadLibSVMFile(sc, "../data/mllib/sample_libsvm_data.txt")
   // val data = sqlContext.read.format("libsvm").load("../data/mllib/sample_libsvm_data.txt")
   val data = sqlContext.createDataFrame(dataSVM)
    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)//fit()方法将DataFrame转化为一个Transformer的算法
    // Automatically identify categorical features, and index them.
    //VectorIndexer是对数据集特征向量中的类别(离散值)特征进行编号
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      //具有4个不同值的特征被为连续
      .setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
      .fit(data)//fit()方法将DataFrame转化为一个Transformer的算法

    // Split the data into training and test sets (30% held out for testing).
    //将数据分成训练和测试集(30%测试)
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    // Train a DecisionTree model.
    //训练一个决策树模型
    val dt = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")//标签列名
      //训练数据集 DataFrame 中存储特征数据的列名
      .setFeaturesCol("indexedFeatures")

    // Convert indexed labels back to original labels.
    //转换索引标签回到原来的标签
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    // Chain indexers and tree in a Pipeline.
     //PipeLine:将多个DataFrame和Estimator算法串成一个特定的ML Wolkflow
     //一个 Pipeline在结构上会包含一个或多个 PipelineStage,每一个 PipelineStage 都会完成一个任务
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))

    // Train model. This also runs the indexers.
    //fit()方法将DataFrame转化为一个Transformer的算法
    val model = pipeline.fit(trainingData)

    // Make predictions.
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val predictions = model.transform(testData)

    // Select example rows to display.
    //选择要显示的示例行
    /**
     * +--------------+-----+--------------------+
     * |predictedLabel|label|            features|
     * +--------------+-----+--------------------+
     * |           1.0|  1.0|(692,[151,152,153...|
     * |           0.0|  0.0|(692,[129,130,131...|
     * |           0.0|  0.0|(692,[154,155,156...|
     * |           0.0|  0.0|(692,[127,128,129...|
     * |           0.0|  0.0|(692,[151,152,153...|
     * +--------------+-----+--------------------+
     */
    predictions.select("predictedLabel", "label", "features").show(5)

    // Select (prediction, true label) and compute test error.
    //选择(预测,真实标签)和计算测试错误。
    val evaluator = new MulticlassClassificationEvaluator()
    //标签列的名称
      .setLabelCol("indexedLabel")
      //算法预测结果的存储列的名称, 默认是”prediction”
      .setPredictionCol("prediction")
      //F1-Measure是根据准确率Precision和召回率Recall二者给出的一个综合的评价指标
      //测量名称列参数(f1,precision,recall,weightedPrecision,weightedRecall)
      //f1        Test Error = 0.04660856384994316
      //precision Test Error = 0.030303030303030276
      //recall    Test Error = 0.0
      .setMetricName("precision")//准确率
      //评估
    val accuracy = evaluator.evaluate(predictions)
    //println("==="+accuracy)
    println("Test Error = " + (1.0 - accuracy))

    val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
    println("Learned classification tree model:\n" + treeModel.toDebugString)
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println
