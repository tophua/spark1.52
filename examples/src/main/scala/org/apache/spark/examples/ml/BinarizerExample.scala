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
import org.apache.spark.ml.feature.Binarizer
// $example off$
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SQLContext, DataFrame}
/**
 * Binarizer 二值化是根据阀值将连续数值特征转换为0-1特征的过程
 * 特征值大于阀值将映射为1.0,特征值小于等于阀值将映射为0.0
 */
object BinarizerExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BinarizerExample").setMaster("local[4]")
    val sc = new SparkContext(conf)
  
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
 
    // $example on$
    val data = Array((0, 0.1), (1, 0.8), (2, 0.2),(2, 0.4),(2, 0.5),(2, 0.6))
    val dataFrame = sqlContext.createDataFrame(data).toDF("label", "feature")
    /**
     * 二值化
     */
    val binarizer: Binarizer = new Binarizer()
      .setInputCol("feature")
      .setOutputCol("binarized_feature")
      //在二进制分类中设置阈值,范围为[0,1],如果类标签1的估计概率>Threshold,则预测1,否则0
      .setThreshold(0.5)
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val binarizedDataFrame = binarizer.transform(dataFrame)
    val binarizedFeatures = binarizedDataFrame.select("binarized_feature")
    /**
    *[0.0]
    *[1.0]
    *[0.0]
    *[0.0]
    *[0.0]
    *[1.0]
    */
    binarizedFeatures.collect().foreach(println)
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println
