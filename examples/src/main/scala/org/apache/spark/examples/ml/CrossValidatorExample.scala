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

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import scala.beans.BeanInfo
/**
 * A simple example demonstrating model selection using CrossValidator.
 * 演示模型选择使用交叉验证一个简单的例子
 * This example also demonstrates how Pipelines are Estimators.
 * 这个例子还演示了如何管道估计
 * This example uses the [[LabeledDocument]] and [[Document]] case classes from
 * [[SimpleTextClassificationPipeline]].
 *
 * Run with
 * {{{
 * bin/run-example ml.CrossValidatorExample
 * }}}
 * 
 * CrossValidator将数据集划分为若干子集分别地进行训练和测试。
 * 如当k＝3时,CrossValidator产生3个训练数据与测试数据对,每个数据对使用2/3的数据来训练,1/3的数据来测试。
 */


object CrossValidatorExample {
/**
 * 如下面例子中,参数网格hashingTF.numFeatures有3个值,lr.regParam有2个值,
 * CrossValidator使用2折交叉验证。这样就会产生(3*2)*2=12中不同的模型需要进行训练
 */
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CrossValidatorExample").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // Prepare training documents, which are labeled.
    //准备训练数据
    val training = sc.parallelize(Seq(
      //id||内容||分类标识
      LabeledDocument(0L, "a b c d e spark", 1.0),
      LabeledDocument(1L, "b d", 0.0),
      LabeledDocument(2L, "spark f g h", 1.0),
      LabeledDocument(3L, "hadoop mapreduce", 0.0),
      LabeledDocument(4L, "b spark who", 1.0),
      LabeledDocument(5L, "g d a y", 0.0),
      LabeledDocument(6L, "spark fly", 1.0),
      LabeledDocument(7L, "was mapreduce", 0.0),
      LabeledDocument(8L, "e spark program", 1.0),
      LabeledDocument(9L, "a e c l", 0.0),
      LabeledDocument(10L, "spark compile", 1.0),
      LabeledDocument(11L, "hadoop software", 0.0)))

    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
      //配置一个ML的管道,其中包括三个阶段, tokenizer, hashingTF, and lr
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")
      //特征提取和转换 TF-IDF
    val hashingTF = new HashingTF()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")
    //逻辑回归
    val lr = new LogisticRegression()
      .setMaxIter(10)
      //PipeLine:将多个DataFrame和Estimator算法串成一个特定的ML Wolkflow
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, lr))

    // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
     //我们现在把管道作为一个估计,包装在一个交叉验证实例
    // This will allow us to jointly choose parameters for all Pipeline stages.
      //他将允许我们共同选择所有管道阶段的参数
    // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
      //交叉验证需要估计,一套估计参数的Map,和一个计算器
    val crossval = new CrossValidator()//交叉
      .setEstimator(pipeline)//表示从一个schemardd构建机器学习模式即Model的逻辑
      //二分类评估
      .setEvaluator(new BinaryClassificationEvaluator)
    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    //我们用一个参数网格生成器构建网格参数搜索
    // With 3 values for hashingTF.numFeatures and 2 values for lr.regParam,
    //
    // this grid will have 3 x 2 = 6 parameter settings for CrossValidator to choose from.
     //该网格将有3 x 2 = 6参数设置选择交叉验证
    val paramGrid = new ParamGridBuilder()//通过addGrid添加我们需要寻找的最佳参数  
      .addGrid(hashingTF.numFeatures, Array(10, 100, 1000))
      .addGrid(lr.regParam, Array(0.1, 0.01))//正则化参数
      .build()
    crossval.setEstimatorParamMaps(paramGrid)//设置构建参数
    crossval.setNumFolds(2) // Use 3+ in practice 在实践中使用3 +

    // Run cross-validation, and choose the best set of parameters.
    //运行交叉验证,并选择最佳的参数集
    //fit()方法将DataFrame转化为一个Transformer的算法
    val cvModel = crossval.fit(training.toDF())

    // Prepare test documents, which are unlabeled.
    //准备测试文档,这些文件是未标记的
    val test = sc.parallelize(Seq(
      Document(4L, "spark i j k"),
      Document(5L, "l m n"),
      Document(6L, "mapreduce spark"),
      Document(7L, "apache hadoop")))

    // Make predictions on test documents.对测试文档进行预测 
    //cvModel uses the best model found (lrModel).C-V模型采用最好的模型
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    cvModel.transform(test.toDF())
      .select("id", "text", "probability", "prediction")
      .collect()
      .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
      println(s"($id, $text) --> prob=$prob, prediction=$prediction")
    }

    sc.stop()
  }
}
// scalastyle:on println
