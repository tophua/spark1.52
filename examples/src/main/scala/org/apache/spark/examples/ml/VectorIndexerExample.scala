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
import org.apache.spark.ml.feature.VectorIndexer
// $example off$
import org.apache.spark.mllib.linalg.Vectors
// $example off$
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.mllib.util._
/**
 * VectorIndexer主要作用:提高决策树或随机森林等ML方法的分类效果。
 */
object VectorIndexerExample {
  def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("TfIdfExample").setMaster("local[4]")
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
   val datasvm=MLUtils.loadLibSVMFile(sc,"../data/mllib/sample_libsvm_data.txt")
   val data =sqlContext.createDataFrame(datasvm)
    //val data = sqlContext.read.format("libsvm").load("../data/mllib/sample_libsvm_data.txt")
/**
 * VectorIndexer是对数据集特征向量中的类别(离散值)特征进行编号
 * 它能够自动判断那些特征是离散值型的特征,并对他们进行编号
 */
    val indexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexed")
      .setMaxCategories(3)//最大类别数为5,(即某一列)中多于10个取值视为连续值

    val indexerModel = indexer.fit(data)

    val categoricalFeatures: Set[Int] = indexerModel.categoryMaps.keys.toSet
    println(s"Chose ${categoricalFeatures.size} categorical features: " +
      categoricalFeatures.mkString(", "))

    // Create new column "indexed" with categorical values transformed to indices
    val indexedData = indexerModel.transform(data)
    /**
     * +-----+--------------------+--------------------+
     * |label|            features|             indexed|
     * +-----+--------------------+--------------------+
     * |  0.0|(692,[127,128,129...|(692,[127,128,129...|
     * |  1.0|(692,[158,159,160...|(692,[158,159,160...|
     * |  1.0|(692,[124,125,126...|(692,[124,125,126...|
     * |  1.0|(692,[152,153,154...|(692,[152,153,154...|
     * |  1.0|(692,[151,152,153...|(692,[151,152,153...|
     * |  0.0|(692,[129,130,131...|(692,[129,130,131...|
     */
    indexedData.show()
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println
