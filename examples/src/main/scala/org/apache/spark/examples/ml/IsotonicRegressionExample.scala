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
import org.apache.spark.ml.regression.IsotonicRegression
// $example off$

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SQLContext, DataFrame}
/**
 * An example demonstrating Isotonic Regression.
 * 保序回归是回归算法的一种
 * Run with
 * {{{
 * bin/run-example ml.IsotonicRegressionExample
 * }}}
 */
object IsotonicRegressionExample {

  def main(args: Array[String]): Unit = {

    // Creates a SparkSession.
   val conf = new SparkConf().setAppName("IsotonicRegressionExample").setMaster("local[4]")
    val sc = new SparkContext(conf)
  
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // $example on$
    // Loads data.
    import org.apache.spark.mllib.util.MLUtils
      val dataSVM=MLUtils.loadLibSVMFile(sc, "../data/mllib/sample_isotonic_regression_libsvm_data.txt")
      val dataset = sqlContext.createDataFrame(dataSVM)
    //val dataset = sqlContext.read.format("libsvm").load("data/mllib/sample_isotonic_regression_libsvm_data.txt")
    /**
     *  libSVM的数据格式
     *  <label> <index1>:<value1> <index2>:<value2> ...
     *  其中<label>是训练数据集的目标值,对于分类,它是标识某类的整数(支持多个类);对于回归,是任意实数
     *  <index>是以1开始的整数,可以是不连续
     *  <value>为实数,也就是我们常说的自变量
     */
      

    // Trains an isotonic regression model.
    val ir = new IsotonicRegression()
    //fit()方法将DataFrame转化为一个Transformer的算法
    val model = ir.fit(dataset)
   //Boundaries in increasing order: [0.01,0.17,0.18,0.27,0.28,0.29,0.3,0.31,0.34,0.35,0.36,0.41,0.42,0.71,0.72,0.74,0.75,0.76,0.77,0.78,0.79,0.8,0.81,0.82,0.83,0.84,0.85,0.86,0.87,0.88,0.89,1.0]
   //Predictions associated with the boundaries: [0.15715271294117658,0.15715271294117658,0.18913819599999995,0.18913819599999995,0.20040796,0.29576747,0.43396226,0.5081591025000001,0.5081591025000001,0.54156043,0.5504844466666667,0.5504844466666667,0.5639299669999999,0.5639299669999999,0.5660377366666668,0.5660377366666668,0.56603774,0.57929628,0.64762876,0.66241713,0.67210607,0.67210607,0.674655785,0.674655785,0.73890872,0.73992861,0.84242733,0.89673636,0.89673636,0.90719021,0.9272055075000002,0.9272055075000002]
    println(s"Boundaries in increasing order: ${model.boundaries}")
    println(s"Predictions associated with the boundaries: ${model.predictions}")

    // Makes predictions.
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    /**
     *+----------+--------------+-------------------+
      |     label|      features|         prediction|
      +----------+--------------+-------------------+
      |0.24579296|(1,[0],[0.01])|0.15715271294117658|
      |0.28505864|(1,[0],[0.02])|0.15715271294117658|
      |0.31208567|(1,[0],[0.03])|0.15715271294117658|
      |0.35900051|(1,[0],[0.04])|0.15715271294117658|
      |0.35747068|(1,[0],[0.05])|0.15715271294117658|
      +----------+--------------+-------------------+
     */
    model.transform(dataset).show(5)
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println
