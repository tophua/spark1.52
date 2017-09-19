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
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
// $example off$
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SQLContext, DataFrame}
/**
 * TF-IDF算法从文本分词中创建特征向量,F-IDF反映了语料中单词对文档的重要程度,
 * 假设单词用t表示,文档用d表示,语料用D表示,那么文档频度DF(t, D)是包含单词t的文档数。
 *  */
object TfIdfExample {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TfIdfExample").setMaster("local[4]")
    val sc = new SparkContext(conf)
  
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // $example on$
    val sentenceData = sqlContext.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (0, "I wish Java could use case classes"),
      (1, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val wordsData = tokenizer.transform(sentenceData)
    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
    val featurizedData = hashingTF.transform(wordsData)
    /**
      +-----+--------------------+--------------------+--------------------+
      |label|            sentence|               words|         rawFeatures|
      +-----+--------------------+--------------------+--------------------+
      |    0|Hi I heard about ...|[hi, i, heard, ab...|(20,[5,6,9],[2.0,...|
      |    0|I wish Java could...|[i, wish, java, c...|(20,[3,5,12,14,18...|
      |    1|Logistic regressi...|[logistic, regres...|(20,[5,12,14,18],...|
      +-----+--------------------+--------------------+--------------------+
     */
    featurizedData.show()
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    //fit()方法将DataFrame转化为一个Transformer的算法
    val idfModel = idf.fit(featurizedData)
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val rescaledData = idfModel.transform(featurizedData)
    /**
    +-----+--------------------+--------------------+--------------------+--------------------+
    |label|            sentence|               words|         rawFeatures|            features|
    +-----+--------------------+--------------------+--------------------+--------------------+
    |    0|Hi I heard about ...|[hi, i, heard, ab...|(20,[5,6,9],[2.0,...|(20,[5,6,9],[0.0,...|
    |    0|I wish Java could...|[i, wish, java, c...|(20,[3,5,12,14,18...|(20,[3,5,12,14,18...|
    |    1|Logistic regressi...|[logistic, regres...|(20,[5,12,14,18],...|(20,[5,12,14,18],...|
    +-----+--------------------+--------------------+--------------------+--------------------+*/
    rescaledData.show()
    rescaledData.select("features", "label").take(3).foreach(println)
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println
