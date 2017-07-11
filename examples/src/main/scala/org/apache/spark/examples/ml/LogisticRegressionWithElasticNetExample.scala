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
// $example off$
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SQLContext, DataFrame}

object LogisticRegressionWithElasticNetExample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogisticRegressionWithElasticNetExample").setMaster("local[4]")
    val sc = new SparkContext(conf)
  
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // $example on$
    // Load training data
    /**
 *  libSVM的数据格式
 *  <label> <index1>:<value1> <index2>:<value2> ...
 *  其中<label>是训练数据集的目标值,对于分类,它是标识某类的整数(支持多个类);对于回归,是任意实数
 *  <index>是以1开始的整数,可以是不连续
 *  <value>为实数,也就是我们常说的自变量
 */
    //val training = sqlContext.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")
    import org.apache.spark.mllib.util.MLUtils
      val dataSVM=MLUtils.loadLibSVMFile(sc, "../data/mllib/sample_libsvm_data.txt")
      val training = sqlContext.createDataFrame(dataSVM)
    val lr = new LogisticRegression()
      .setMaxIter(10)//最大迭代次数
      .setRegParam(0.3)//正则化参数
      .setElasticNetParam(0.8)//弹性网络混合参数,0.0为L2正则化 1.0为L1正则化

    // Fit the model
    //fit()方法将DataFrame转化为一个Transformer的算法
    val lrModel = lr.fit(training)

    // Print the coefficients and intercept for logistic regression
    //println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println
