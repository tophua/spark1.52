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
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SQLContext, DataFrame}
// $example off$

/**
 * 朴素贝叶斯法是基于贝叶斯定理与特征条件独立假设的分类方法
 */
object NaiveBayesExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CrossValidatorExample").setMaster("local[4]")
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
    //val data = sqlContext.read.format("libsvm").load("../data/mllib/sample_libsvm_data.txt")
    import org.apache.spark.mllib.util.MLUtils
      val dataSVM=MLUtils.loadLibSVMFile(sc, "../data/mllib/sample_libsvm_data.txt")
      val data = sqlContext.createDataFrame(dataSVM)
    // Split the data into training and test sets (30% held out for testing)
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3), seed = 1234L)

    // Train a NaiveBayes model.
    //训练一个朴素贝叶斯模型
    val model = new NaiveBayes()
    //fit()方法将DataFrame转化为一个Transformer的算法
      .fit(trainingData)

    // Select example rows to display.
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val predictions = model.transform(testData)
    //rawPrediction原始的算法预测结果的存储列的名称
    //probability 类别预测结果的条件概率值存储列的名称
    //prediction 算法预测结果的存储列的名称
    /**
      +-----+--------------------+--------------------+-----------+----------+
      |label|            features|       rawPrediction|probability|prediction|
      +-----+--------------------+--------------------+-----------+----------+
      |  0.0|(692,[127,128,129...|[-169346.60693004...|  [1.0,0.0]|       0.0|
      |  1.0|(692,[158,159,160...|[-122090.84503412...|  [0.0,1.0]|       1.0|
      |  1.0|(692,[124,125,126...|[-127511.92919768...|  [0.0,1.0]|       1.0|
      |  1.0|(692,[152,153,154...|[-80786.287309771...|  [0.0,1.0]|       1.0|
      |  0.0|(692,[153,154,155...|[-260955.28182841...|  [1.0,0.0]|       0.0|
      +-----+--------------------+--------------------+-----------+----------+*/
    predictions.show(5)

    // Select (prediction, true label) and compute test error
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")//标签列名
      .setPredictionCol("prediction")//预测结果列名
      .setMetricName("precision")//准确率
    //Accuracy: 1.0
    val accuracy = evaluator.evaluate(predictions)
    println("Accuracy: " + accuracy)
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println
