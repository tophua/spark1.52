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
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row
// $example off$

// $example off$
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SQLContext, DataFrame}
/**
 * 评估转换参数例子
 */
object EstimatorTransformerParamExample {

  def main(args: Array[String]): Unit = {
    
     val conf = new SparkConf().setAppName("EstimatorTransformerParamExample").setMaster("local[4]")
    val sc = new SparkContext(conf)
  
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
   

    // $example on$
    // Prepare training data from a list of (label, features) tuples.
    // 准备训练来自一旬(标签,特征)的元组
    val training = sqlContext.createDataFrame(Seq(
      (1.0, Vectors.dense(0.0, 1.1, 0.1)),
      (0.0, Vectors.dense(2.0, 1.0, -1.0)),
      (0.0, Vectors.dense(2.0, 1.3, 1.0)),
      (1.0, Vectors.dense(0.0, 1.2, -0.5))
    )).toDF("label", "features")

    // Create a LogisticRegression instance. This instance is an Estimator.
    //创建一个逻辑回归实例,实例化一个评估
    val lr = new LogisticRegression()
    // Print out the parameters, documentation, and any default values.
     //打印输出参数,文档和一些默认值
    println("LogisticRegression parameters:\n" + lr.explainParams() + "\n")

    // We may set parameters using setter methods.
    //设置参数使用set方法
    lr.setMaxIter(10)
      .setRegParam(0.01)

    // Learn a LogisticRegression model. This uses the parameters stored in lr.
    //fit()方法将DataFrame转化为一个Transformer的算法
    val model1 = lr.fit(training)
    // Since model1 is a Model (i.e., a Transformer produced by an Estimator),
    // we can view the parameters it used during fit().
    // This prints the parameter (name: value) pairs, where names are unique IDs for this
    // 打印参数部分(名称:值),这里的名字逻辑回归实例唯一识
    // LogisticRegression instance.
    /**
     * Model 1 was fit using parameters: {
      	logreg_d48f0302d9e8-elasticNetParam: 0.0,
      	logreg_d48f0302d9e8-featuresCol: features,
      	logreg_d48f0302d9e8-fitIntercept: true,
      	logreg_d48f0302d9e8-labelCol: label,
      	logreg_d48f0302d9e8-maxIter: 10,
      	logreg_d48f0302d9e8-predictionCol: prediction,
      	logreg_d48f0302d9e8-probabilityCol: probability,
      	logreg_d48f0302d9e8-rawPredictionCol: rawPrediction,
      	logreg_d48f0302d9e8-regParam: 0.01,
      	logreg_d48f0302d9e8-standardization: true,
      	logreg_d48f0302d9e8-threshold: 0.5,
      	logreg_d48f0302d9e8-tol: 1.0E-6
      }
     */
    println("Model 1 was fit using parameters: " + model1.parent.extractParamMap)

    // We may alternatively specify parameters using a ParamMap,
    //使用parammap指定参数
    // which supports several methods for specifying parameters.
    //支持指定参数的几种方法
    val paramMap = ParamMap(lr.maxIter -> 20)
      .put(lr.maxIter, 30)  // Specify 1 Param. This overwrites the original maxIter.
      //在二进制分类中设置阈值,范围为[0,1],如果类标签1的估计概率>Threshold,则预测1,否则0
      .put(lr.regParam -> 0.1, lr.threshold -> 0.55)  // Specify multiple Params.

    // One can also combine ParamMaps.
    val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability")  // Change output column name.
    val paramMapCombined = paramMap ++ paramMap2

    // Now learn a new model using the paramMapCombined parameters.
    // paramMapCombined overrides all parameters set earlier via lr.set* methods.
    //fit()方法将DataFrame转化为一个Transformer的算法
    val model2 = lr.fit(training, paramMapCombined)
    /**
     * Model 2 was fit using parameters: {
      	logreg_d48f0302d9e8-elasticNetParam: 0.0,
      	logreg_d48f0302d9e8-featuresCol: features,
      	logreg_d48f0302d9e8-fitIntercept: true,
      	logreg_d48f0302d9e8-labelCol: label,
      	logreg_d48f0302d9e8-maxIter: 30,
      	logreg_d48f0302d9e8-predictionCol: prediction,
      	logreg_d48f0302d9e8-probabilityCol: myProbability,
      	logreg_d48f0302d9e8-rawPredictionCol: rawPrediction,
      	logreg_d48f0302d9e8-regParam: 0.1,
      	logreg_d48f0302d9e8-standardization: true,
      	logreg_d48f0302d9e8-threshold: 0.55,
      	logreg_d48f0302d9e8-tol: 1.0E-6
      }
     */
    println("Model 2 was fit using parameters: " + model2.parent.extractParamMap)

    // Prepare test data.
    //准备测试数据
    val test = sqlContext.createDataFrame(Seq(
      (1.0, Vectors.dense(-1.0, 1.5, 1.3)),
      (0.0, Vectors.dense(3.0, 2.0, -0.1)),
      (1.0, Vectors.dense(0.0, 2.2, -1.5))
    )).toDF("label", "features")

    // Make predictions on test data using the Transformer.transform() method.
    // LogisticRegression.transform will only use the 'features' column.
    // Note that model2.transform() outputs a 'myProbability' column instead of the usual
    // 'probability' column since we renamed the lr.probabilityCol parameter previously.
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    model2.transform(test)
      //类别预测结果的条件概率值存储列的名称,默认值是”probability”
      //算法预测结果的存储列的名称,默认是”prediction”
      .select("features", "label", "myProbability", "prediction")
      .collect()
      .foreach { case Row(features: Vector, label: Double, prob: Vector, prediction: Double) =>
        /**
         *([-1.0,1.5,1.3], 1.0) -> prob=[0.05707304171034022,0.9429269582896597], prediction=1.0
          ([3.0,2.0,-0.1], 0.0) -> prob=[0.9238522311704104,0.07614776882958964], prediction=0.0
          ([0.0,2.2,-1.5], 1.0) -> prob=[0.10972776114779435,0.8902722388522056], prediction=1.0
         */
        println(s"($features, $label) -> prob=$prob, prediction=$prediction")
      }
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println
