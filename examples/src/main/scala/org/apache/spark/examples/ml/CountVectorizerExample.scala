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
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
// $example off$
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SQLContext, DataFrame}
/**
 * Countvectorizer和Countvectorizermodel旨在通过计数来将一个文档转换为向量
 */
object CountVectorizerExample {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CountVectorizerExample").setMaster("local[4]")
    val sc = new SparkContext(conf)
  
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    // $example on$
    //DataFrame包含id和texts两列
    val df = sqlContext.createDataFrame(Seq(
      (0, Array("a", "b", "c")),
      (1, Array("a", "b", "b", "c", "a"))
    )).toDF("id", "words")

    // fit a CountVectorizerModel from the corpus
    //文本中的每一行都是一个文档类型的数组(字符串),调用的CountVectorizer产生词汇(a,b,c)的CountVectorizerModel,
    //转换后的输出向量如下
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(3)//是以词为键,并且值可以在特征矩阵里可以索引的
      .setMinDF(2)
      .fit(df)//fit()方法将DataFrame转化为一个Transformer的算法
    /**
     * +--------------------+
     * |            features|
     * +--------------------+
     * |(3,[0,1,2],[1.0,1...|
     * |(3,[0,1,2],[2.0,2...|
     * +--------------------+
     */
     //transform()方法将DataFrame转化为另外一个DataFrame的算法
      cvModel.transform(df).select("features").show()
    // alternatively, define CountVectorizerModel with a-priori vocabulary
    val cvm = new CountVectorizerModel(Array("a", "b", "c"))
      .setInputCol("words")
      .setOutputCol("features")
    /**
     * +--------------------+
     * |            features|
     * +--------------------+
     * |(3,[0,1,2],[1.0,1...|
     * |(3,[0,1,2],[2.0,2...|
     * +--------------------+
     */
     //transform()方法将DataFrame转化为另外一个DataFrame的算法
    cvm.transform(df).select("features").show()
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println


