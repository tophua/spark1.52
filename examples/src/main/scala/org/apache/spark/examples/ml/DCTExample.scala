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
import org.apache.spark.ml.feature.DCT
import org.apache.spark.mllib.linalg.Vectors
// $example off$
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SQLContext, DataFrame}

object DCTExample {
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("DCTExample").setMaster("local[4]")
    val sc = new SparkContext(conf)
  
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    
   

    // $example on$
    val data = Seq(
      Vectors.dense(0.0, 1.0, -2.0, 3.0),
      Vectors.dense(-1.0, 2.0, 4.0, -7.0),
      Vectors.dense(14.0, -2.0, -5.0, 1.0))

    val df = sqlContext.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    //离散余弦变换(DCT)
    val dct = new DCT()
      .setInputCol("features")
      .setOutputCol("featuresDCT")
      .setInverse(false)
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val dctDf = dct.transform(df)
    /**
     * +--------------------+
     * |         featuresDCT|
     * +--------------------+
     * |[1.0,-1.148050297...|
     * |[-1.0,3.378492794...|
     * |[4.0,9.3044534219...|
     * +--------------------+
     */
    dctDf.select("featuresDCT").show(3)
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println

