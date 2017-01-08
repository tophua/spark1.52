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
import org.apache.spark.ml.feature.PolynomialExpansion
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.Row
// $example off$
import org.apache.spark.sql.Row
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SQLContext, DataFrame}
/**
 * PolynomialExpansion 多项式扩展通过产生n维组合将原始特征将特征扩展到多项式空间
 */
object PolynomialExpansionExample {
  def main(args: Array[String]): Unit = {
       val conf = new SparkConf().setAppName("PolynomialExpansionExample").setMaster("local[4]")
    val sc = new SparkContext(conf)
  
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // $example on$
    /**
     * 下面的示例会介绍如何将你的特征集拓展到3维多项式空间
     */
    val data = Array(
      Vectors.dense(-2.0, 2.3),
      Vectors.dense(0.0, 0.0),
      Vectors.dense(0.6, -1.1)
    )
    val df = sqlContext.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    val polynomialExpansion = new PolynomialExpansion()
      .setInputCol("features")
      .setOutputCol("polyFeatures")
      .setDegree(3)
      //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val polyDF = polynomialExpansion.transform(df)
    /**
    *[[-2.0,4.0,-8.0,2.3,-4.6,9.2,5.289999999999999,-10.579999999999998,12.166999999999996]]
    *[[0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0]]
    *[[0.6,0.36,0.216,-1.1,-0.66,-0.396,1.2100000000000002,0.7260000000000001,-1.3310000000000004]]
    */
    polyDF.select("polyFeatures").take(3).foreach(println)
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println
