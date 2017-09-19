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

package org.apache.spark.mllib.feature

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vectors, Vector}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
/**
 * HashTF从一个文档中计算出给定大小的词频向量。为了将词和向量顺序对应起来,所以使用了哈希。
 * HashingTF使用每个单词对所需向量的长度S取模得出的哈希值,把所有单词映射到一个0到S-1之间的数字上。
 * 由此可以保证生成一个S维的向量。随后当构建好词频向量后,使用IDF来计算逆文档频率,然后将它们与词频相乘计算TF-IDF
 * TF-IDF算法从文本分词中创建特征向量
 * TF-IDF是一种统计方法,用以评估一字词对于一个文件集或一个语料库中的其中一份文件的重要程度.
 * 字词的重要性随着它在文件中出现的次数成正比增加,但同时会随着它在语料库中出现的频率成反比下降
 */
class IDFSuite extends SparkFunSuite with MLlibTestSparkContext {
/**
 * IDF计算提供了忽略低频词的选项。被忽略的词IDF置零。该特性可以通过将参数minDocFreq传给IDF构造函数来使用。
 */
  test("idf") {
    val n = 4
    val localTermFrequencies = Seq(
      Vectors.sparse(n, Array(1, 3), Array(1.0, 2.0)),
      Vectors.dense(0.0, 1.0, 2.0, 3.0),
      Vectors.sparse(n, Array(1), Array(1.0))
    )
   
    val m = localTermFrequencies.size
    val termFrequencies = sc.parallelize(localTermFrequencies, 2)
    val idf = new IDF
    //termFrequencies List((4,[1,3],[1.0,2.0]), [0.0,1.0,2.0,3.0], (4,[1],[1.0]))
    //fit()方法将DataFrame转化为一个Transformer的算法
    val model = idf.fit(termFrequencies)
    val expected = Vectors.dense(Array(0, 3, 1, 2).map { x =>
      math.log((m + 1.0) / (x + 1.0))//对数
    })
    println(expected)
    //  0,                3,   1,                 2 重要程度 
    //[1.3862943611198906,0.0,0.6931471805599453,0.28768207245178085]
    assert(model.idf ~== expected absTol 1e-12)

    val assertHelper = (tfidf: Array[Vector]) => {
      assert(tfidf.size === 3)
      val tfidf0 = tfidf(0).asInstanceOf[SparseVector]
      //indices 指数
      assert(tfidf0.indices === Array(1, 3))      
      //tfidf0.values [0.0,0.5753641449035617]
      println("tfidf0.values>>>>>>:"+Vectors.dense(tfidf0.values))
      assert(Vectors.dense(tfidf0.values) ~==
          Vectors.dense(1.0 * expected(1), 2.0 * expected(3)) absTol 1e-12)
      val tfidf1 = tfidf(1).asInstanceOf[DenseVector]
      assert(Vectors.dense(tfidf1.values) ~==
          Vectors.dense(0.0, 1.0 * expected(1), 2.0 * expected(2), 3.0 * expected(3)) absTol 1e-12)
      val tfidf2 = tfidf(2).asInstanceOf[SparseVector]
      assert(tfidf2.indices === Array(1))
      //[0.0]
        println("tfidf2.values>>>>>>:"+Vectors.dense(tfidf2.values))
      assert(tfidf2.values(0) ~== (1.0 * expected(1)) absTol 1e-12)
    }
    // Transforms a RDD 转换一个RDD
     //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val tfidf = model.transform(termFrequencies).collect()
    //WrappedArray((4,[1,3],[0.0,0.5753641449035617]), [0.0,0.0,1.3862943611198906,0.8630462173553426], (4,[1],[0.0]))
    println("Transforms:"+tfidf.toSeq)
    assertHelper(tfidf)
    // Transforms local vectors 转换本地向量
    //((4,[1,3],[0.0,0.5753641449035617]), [0.0,0.0,1.3862943611198906,0.8630462173553426], (4,[1],[0.0]))
     //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val localTfidf = localTermFrequencies.map(model.transform(_)).toArray
     println("localTfidf:"+localTfidf.toSeq)
    assertHelper(localTfidf)
  }

  test("idf minimum document frequency filtering") {//IDF最小文档频率过虑
    val n = 4
    val localTermFrequencies = Seq(
      Vectors.sparse(n, Array(1, 3), Array(1.0, 2.0)),
      Vectors.dense(0.0, 1.0, 2.0, 3.0),
      Vectors.sparse(n, Array(1), Array(1.0))
    )
    val m = localTermFrequencies.size
    val termFrequencies = sc.parallelize(localTermFrequencies, 2)
    val idf = new IDF(minDocFreq = 1)
    //fit()方法将DataFrame转化为一个Transformer的算法
    val model = idf.fit(termFrequencies)
    val expected = Vectors.dense(Array(0, 3, 1, 2).map { x =>
      if (x > 0) {
        math.log((m + 1.0) / (x + 1.0))
      } else {
        0
      }
    })
    println("expected:"+expected)
    //[0.0,0.0,0.6931471805599453,0.28768207245178085]
    assert(model.idf ~== expected absTol 1e-12)

    val assertHelper = (tfidf: Array[Vector]) => {
      assert(tfidf.size === 3)
      val tfidf0 = tfidf(0).asInstanceOf[SparseVector]
      assert(tfidf0.indices === Array(1, 3))
      assert(Vectors.dense(tfidf0.values) ~==
          Vectors.dense(1.0 * expected(1), 2.0 * expected(3)) absTol 1e-12)
      val tfidf1 = tfidf(1).asInstanceOf[DenseVector]
      assert(Vectors.dense(tfidf1.values) ~==
          Vectors.dense(0.0, 1.0 * expected(1), 2.0 * expected(2), 3.0 * expected(3)) absTol 1e-12)
      val tfidf2 = tfidf(2).asInstanceOf[SparseVector]
      assert(tfidf2.indices === Array(1))
      assert(tfidf2.values(0) ~== (1.0 * expected(1)) absTol 1e-12)
    }
    // Transforms a RDD
   //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val tfidf = model.transform(termFrequencies).collect()
    assertHelper(tfidf)
    // Transforms local vectors
    // 转换本地向量
    val localTfidf = localTermFrequencies.map(model.transform(_)).toArray
    assertHelper(localTfidf)
  }

}
