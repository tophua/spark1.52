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
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Row, SQLContext}

/**
 * A simple example demonstrating ways to specify parameters for Estimators and Transformers.
 * 一个简单的例子演示的方法来指定参数估计和转换
 * Run with
 * {{{
 * bin/run-example ml.SimpleParamsExample
 * }}}
 */
object SimpleParamsExample {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SimpleParamsExample").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // Prepare training data.准备训练数据
    // We use LabeledPoint, which is a case class.  Spark SQL can convert RDDs of case classes
    //我们使用标记点,这是一个案例类,Spark SQL可以转换案例课RDDs成数据帧
    // into DataFrames, where it uses the case class metadata to infer the schema.
    //它使用案例类的元数据来推断模式
    val training = sc.parallelize(Seq(
      LabeledPoint(1.0, Vectors.dense(0.0, 1.1, 0.1)),
      LabeledPoint(0.0, Vectors.dense(2.0, 1.0, -1.0)),
      LabeledPoint(0.0, Vectors.dense(2.0, 1.3, 1.0)),
      LabeledPoint(1.0, Vectors.dense(0.0, 1.2, -0.5))))

    // Create a LogisticRegression instance.  This instance is an Estimator.
    //创建一个逻辑回归实例,这个实例是一个估计量
    val lr = new LogisticRegression()
    // Print out the parameters, documentation, and any default values.
    //打印参数、文档和任何默认值
    println("LogisticRegression parameters:\n" + lr.explainParams() + "\n")

    // We may set parameters using setter methods.
    //我们可以使用setter方法设置参数
    lr.setMaxIter(10)
      .setRegParam(0.01)

    // Learn a LogisticRegression model.  This uses the parameters stored in lr.
    //学习一个逻辑回归模型,这使用存储的参数
    val model1 = lr.fit(training.toDF())//fit()方法将DataFrame转化为一个Transformer的算法
    // Since model1 is a Model (i.e., a Transformer produced by an Estimator),由估计量产生的转换
    // we can view the parameters it used during fit().
    // This prints the parameter (name: value) pairs, where names are unique IDs for this
    //此打印参数(名称:值)对,在这个逻辑回归实例中，名称是唯一的标识
    // LogisticRegression instance.
    println("Model 1 was fit using parameters: " + model1.parent.extractParamMap())

    // We may alternatively specify parameters using a ParamMap,
    //我们可以交替使用parammap指定参数
    // which supports several methods for specifying parameters.
    //它支持几种指定参数的方法
    val paramMap = ParamMap(lr.maxIter -> 20)
    //指定1个参数,这将覆盖原maxiter。
    paramMap.put(lr.maxIter, 30) // Specify 1 Param.  This overwrites the original maxIter.
    paramMap.put(lr.regParam -> 0.1, lr.thresholds -> Array(0.45, 0.55)) // Specify multiple Params.

    // One can also combine ParamMaps. 你也可以将参数映射
    val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability") // Change output column name 更改输出列名称
    val paramMapCombined = paramMap ++ paramMap2

    // Now learn a new model using the paramMapCombined parameters.
    //现在学习一个新的模型使用parammapcombined参数
    // paramMapCombined overrides all parameters set earlier via lr.set* methods.
    //覆盖所有参数设置之前通过LR定方法
    val model2 = lr.fit(training.toDF(), paramMapCombined)
    println("Model 2 was fit using parameters: " + model2.parent.extractParamMap())

    // Prepare test data.准备测试数据
    val test = sc.parallelize(Seq(
      LabeledPoint(1.0, Vectors.dense(-1.0, 1.5, 1.3)),
      LabeledPoint(0.0, Vectors.dense(3.0, 2.0, -0.1)),
      LabeledPoint(1.0, Vectors.dense(0.0, 2.2, -1.5))))

    // Make predictions on test data using the Transformer.transform() method.
    // LogisticRegressionModel.transform will only use the 'features' column.
    // Note that model2.transform() outputs a 'myProbability' column instead of the usual
    // 'probability' column since we renamed the lr.probabilityCol parameter previously.
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    model2.transform(test.toDF())
      .select("features", "label", "myProbability", "prediction")
      .collect()
      /**
        ([-1.0,1.5,1.3], 1.0) -> prob=[0.05707304171034022,0.9429269582896597], prediction=1.0
        ([3.0,2.0,-0.1], 0.0) -> prob=[0.9238522311704104,0.07614776882958958], prediction=0.0
        ([0.0,2.2,-1.5], 1.0) -> prob=[0.10972776114779449,0.8902722388522056], prediction=1.0
       */
      .foreach { case Row(features: Vector, label: Double, prob: Vector, prediction: Double) =>
        println(s"($features, $label) -> prob=$prob, prediction=$prediction")
      }

    sc.stop()
  }
}
// scalastyle:on println
