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
import org.apache.spark.ml.feature.ElementwiseProduct
import org.apache.spark.mllib.linalg.Vectors
// $example off$
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SQLContext, DataFrame}
/**
 * ElementwiseProduct点乘,就是说每个矩阵元素对应相乘
 */
object ElementwiseProductExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ElementwiseProductExample").setMaster("local[4]")
    val sc = new SparkContext(conf)
  
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    
   

    // $example on$
    // Create some vector data; also works for sparse vectors
    //创建一些向量数据,也适用于稀疏向量
    val dataFrame = sqlContext.createDataFrame(Seq(
      ("a", Vectors.dense(1.0, 2.0, 3.0)),
      ("b", Vectors.dense(4.0, 5.0, 6.0)))).toDF("id", "vector")

    val transformingVector = Vectors.dense(0.0, 1.0, 2.0)
    //ElementwiseProduct 点乘,就是说每个矩阵元素对应相乘
    val transformer = new ElementwiseProduct()
      .setScalingVec(transformingVector)
      .setInputCol("vector")
      .setOutputCol("transformedVector")

    // Batch transform the vectors to create new column:
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    /**
     * +---+-------------+-----------------+
     * | id|       vector|transformedVector|
     * +---+-------------+-----------------+
     * |  a|[1.0,2.0,3.0]|    [0.0,2.0,6.0]|
     * |  b|[4.0,5.0,6.0]|   [0.0,5.0,12.0]|
     * +---+-------------+-----------------+
     */
    transformer.transform(dataFrame).show()
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println
