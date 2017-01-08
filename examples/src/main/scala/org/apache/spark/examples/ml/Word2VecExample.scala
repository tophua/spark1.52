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
import org.apache.spark.ml.feature.Word2Vec

// $example off$

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SQLContext, DataFrame}
/**
 * Word2vec是一个Estimator,它采用一系列代表文档的词语来训练word2vecmodel,该模型将每个词语映射到一个固定大小的向量
 * word2vecmodel使用文档中每个词语的平均数来将文档转换为向量,然后这个向量可以作为预测的特征,来计算文档相似度计算等等
 */
object Word2VecExample {
  def main(args: Array[String]) {
   val conf = new SparkConf().setAppName("Word2VecExample").setMaster("local[4]")
    val sc = new SparkContext(conf)  
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    // $example on$
    /**
     * 我们首先用一组文档,其中每一个文档代表一个词语序列。对于每一个文档,我们将其转换为一个特征向量。
     * 此特征向量可以被传递到一个学习算法。
     */
    // Input data: Each row is a bag of words from a sentence or document.
    val documentDF = sqlContext.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
     //fit()方法将DataFrame转化为一个Transformer的算法
    val model = word2Vec.fit(documentDF)
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val result = model.transform(documentDF)
    result.select("result").take(3).foreach(println)
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println
