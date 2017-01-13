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
import org.apache.spark.ml.feature.Normalizer
// $example off$
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SQLContext, DataFrame}
/**
 * 标准化是指:对于训练集中的样本,基于列统计信息将数据除以方差或(且)者将数据减去其均值(结果是方差等于1,数据在0附近)
 */
object NormalizerExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CrossValidatorExample").setMaster("local[4]")
    val sc = new SparkContext(conf)
  
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // $example on$
    /**
 *  libSVM的数据格式
 *  <label> <index1>:<value1> <index2>:<value2> ...
 *  其中<label>是训练数据集的目标值,对于分类,它是标识某类的整数(支持多个类);对于回归,是任意实数
 *  <index>是以1开始的整数,可以是不连续
 *  <value>为实数,也就是我们常说的自变量
 */
   // val dataFrame = sqlContext.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")
     import org.apache.spark.mllib.util.MLUtils
      val dataSVM=MLUtils.loadLibSVMFile(sc, "../data/mllib/sample_libsvm_data.txt")
      val dataFrame = sqlContext.createDataFrame(dataSVM)
    // Normalize each Vector using $L^1$ norm.
    //标准化是指:对于训练集中的样本,基于列统计信息将数据除以方差或(且)者将数据减去其均值(结果是方差等于1,数据在0附近)
    val normalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("normFeatures")
      .setP(1.0)
     //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val l1NormData = normalizer.transform(dataFrame)
    /**
    +-----+--------------------+--------------------+
    |label|            features|        normFeatures|
    +-----+--------------------+--------------------+
    |  0.0|(692,[127,128,129...|(692,[127,128,129...|    
    |  1.0|(692,[129,130,131...|(692,[129,130,131...|
    |  0.0|(692,[154,155,156...|(692,[154,155,156...|
    |  1.0|(692,[150,151,152...|(692,[150,151,152...|
    |  0.0|(692,[124,125,126...|(692,[124,125,126...|
    |  0.0|(692,[152,153,154...|(692,[152,153,154...|
    |  1.0|(692,[97,98,99,12...|(692,[97,98,99,12...|
    |  1.0|(692,[124,125,126...|(692,[124,125,126...|
    +-----+--------------------+--------------------+*/
    l1NormData.show()
    // Normalize each Vector using $L^\infty$ norm.
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val lInfNormData = normalizer.transform(dataFrame, normalizer.p -> Double.PositiveInfinity)
    /**
      +-----+--------------------+--------------------+
      |label|            features|        normFeatures|
      +-----+--------------------+--------------------+
      |  0.0|(692,[127,128,129...|(692,[127,128,129...|
      |  1.0|(692,[158,159,160...|(692,[158,159,160...|
      |  1.0|(692,[124,125,126...|(692,[124,125,126...|
      |  1.0|(692,[129,130,131...|(692,[129,130,131...|
      |  0.0|(692,[154,155,156...|(692,[154,155,156...|
      |  1.0|(692,[150,151,152...|(692,[150,151,152...|
      |  0.0|(692,[124,125,126...|(692,[124,125,126...|
      +-----+--------------------+--------------------+*/
    lInfNormData.show()
    // $example off$
    sc.stop()
  }
}
// scalastyle:on println
