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

package org.apache.spark.ml.feature

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.MLTestingUtils
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.mllib.feature.{Word2VecModel => OldWord2VecModel}
/**
 Word2Vec模型来根据词义比较两个字词相似性
 Word2Vec 是一个用来将词表示为数值型向量的工具，其基本思想是将文本中的词映射成一个 K 维数值向量 (K 通常作为算法的超参数)，
 这样文本中的所有词就组成一个 K 维向量空间， 这样我们可以通过计算向量间的欧氏距离或者余弦相似度得到文本语义的相似度。
 Word2Vec 采用的是 Distributed representation 的词向量表示方式，这种表达方式不仅可以有效控制词向量的维度，
 避免维数灾难 (相对于 one-hot representation)，而且可以保证意思相近的词在向量空间中的距离较近。
 */
class Word2VecSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("params") {//参数
    ParamsSuite.checkParams(new Word2Vec)
    val model = new Word2VecModel("w2v", new OldWord2VecModel(Map("a" -> Array(0.0f))))
    ParamsSuite.checkParams(model)
  }

  test("Word2Vec") {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val sentence = "a b " * 100 + "a c " * 10
    //numOfWords: Int = 220
    val numOfWords = sentence.split(" ").size
    /**
     * res21: = Array(Array(a, b, a, b, a, b, a, b, a, b, a, b, a,
       b, a, b, a, b, a, b, a, b, a, b, a, b, a, b, a, b, a, b, a, b, a, b, a, b, a, b
       a, b, a, b, a, b, a, b, a, b, a, b, a, b, a, b, a, b, a, b, a, b, a, b, a, b...
     */
    val doc = sc.parallelize(Seq(sentence, sentence)).map(line => line.split(" "))
   //编码
    /** 
     * codes: scala.collection.immutable.Map[String,Array[Double]] = Map(
     * 	a -> Array(-0.2811822295188904, -0.6356269121170044, -0.3020961284637451),
     *  b -> Array(1.0309048891067505, -1.29472815990448, 0.22276712954044342), 
     *  c -> Array(-0.08456747233867645, 0.5137411952018738, 0.11731560528278351))
     */
    val codes = Map(
      "a" -> Array(-0.2811822295188904, -0.6356269121170044, -0.3020961284637451),
      "b" -> Array(1.0309048891067505, -1.29472815990448, 0.22276712954044342),
      "c" -> Array(-0.08456747233867645, 0.5137411952018738, 0.11731560528278351)
    )

    val expected = doc.map { sentence =>
      Vectors.dense(sentence.map(codes.apply).reduce((word1, word2) =>
        word1.zip(word2).map { case (v1, v2) => v1 + v2 }
      ).map(_ / numOfWords))
    }

    val docDF = doc.zip(expected).toDF("text", "expected")

    val model = new Word2Vec()
      //目标数值向量的维度大小，默认是 100
      .setVectorSize(3)
      //源数据 DataFrame 中存储文本词数组列的名称
      .setInputCol("text")
      //经过处理的数值型特征向量存储列名称
      .setOutputCol("result") 
      //种子
      .setSeed(42L)
      //.minCount 只有当某个词出现的次数大于或者等于 minCount 时，才会被包含到词汇表里，否则会被忽略掉
      .fit(docDF)

    // copied model must have the same parent.
    MLTestingUtils.checkCopy(model)

    model.transform(docDF).select("result", "expected").collect().foreach {
      case Row(vector1: Vector, vector2: Vector) =>
        assert(vector1 ~== vector2 absTol 1E-5, "Transformed vector is different with expected.")
    }
  }

  test("getVectors") {//获得向量

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val sentence = "a b " * 100 + "a c " * 10
    val doc = sc.parallelize(Seq(sentence, sentence)).map(line => line.split(" "))
    val codes = Map(
      "a" -> Array(-0.2811822295188904, -0.6356269121170044, -0.3020961284637451),
      "b" -> Array(1.0309048891067505, -1.29472815990448, 0.22276712954044342),
      "c" -> Array(-0.08456747233867645, 0.5137411952018738, 0.11731560528278351)
    )
    val expectedVectors = codes.toSeq.sortBy(_._1).map { case (w, v) => Vectors.dense(v) }
    val docDF = doc.zip(doc).toDF("text", "alsotext")
    val model = new Word2Vec()
      .setVectorSize(3)//设置向量长度
      .setInputCol("text")//设置输入列
      .setOutputCol("result")//设置输出列  
      .setSeed(42L)//设置种子
      .fit(docDF)

    val realVectors = model.getVectors.sort("word").select("vector").map {
      case Row(v: Vector) => {
       // println("Vector:"+v.toDense)
        v
        }
    }.collect()

    realVectors.zip(expectedVectors).foreach {
      case (real, expected) =>
        //println(real+"||||"+expected)
        assert(real ~== expected absTol 1E-5, "Actual vector is different from expected.")
    }
  }

  test("findSynonyms") {//查找同义词

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    /**
     * sentence: String = "a b a b a b a b a b a b a b a b a b a b a b a b a b a b a b
      a b a b a b a b a b a b a b a b a b a b a b a b a b a b a b a b a b a b a b a b
      a b a b a b a b a b a b a b a b a b a b a b a b a b a b a b a b a b a b a b a b
      a b a b a b a b a b a c a c a c a c a c a c a c a c a c a c "
     */
    val sentence = "a b " * 100 + "a c " * 10
    // val sentence = "中 国  人 民 " * 100 + "同 本  文 时" * 10
    /**
     res17: Array[Array[String]] = Array(Array(a, b, a, b, a, b, a, b, a, b, a, b, a,
       b, a, b, a, b, a, b, a, b, a, b, a, b, a, b, a, b, a, b, a, b, a, b, a, b, a, b
       a, b, a, b, a, b, a, b, a, b, a, b, a, b, a, b, a, b, a, b, a, b, a, b, a, b...
     */
    val doc = sc.parallelize(Seq(sentence, sentence)).map(line => line.split(" "))
    //同时文本
    val docDF = doc.zip(doc).toDF("text", "alsotext")
    //特征提取和转换 Word2Vec
    val model = new Word2Vec().setVectorSize(3).setInputCol("text").setOutputCol("result").setSeed(42L).fit(docDF)
    //查找最相似的2个单词
    val expectedSimilarity = Array(0.2789285076917586, -0.6336972059851644)
    //collect: Array[(String, Double)] = Array((b,0.2789285076917586), (c,-0.6336972059851644))
    /**
     * collect().unzip
     * synonyms: scala.collection.mutable.IndexedSeq[String] = ArrayBuffer(b, c)
			 similarity: IndexedSeq[Double] = ArrayBuffer(0.2789285076917586, -0.6336972059851644)
     */
    val (synonyms, similarity) = model.findSynonyms("a", 2).map {
      case Row(w: String, sim: Double) => 
        /**
         * b||||0.2789285076917586
				 * c||||-0.6336972059851644
         */
        //println(w+"||||"+sim)
        (w, sim)      
    }.collect().unzip

    assert(synonyms.toArray === Array("b", "c"))
    expectedSimilarity.zip(similarity).map {
      case (expected, actual) => 
        //println(expected+"||||"+actual)
        //println("kkkkk:"+(expected - actual))
        //math.abs((expected - actual) / expected)
	////abs返回数的绝对值
        assert(math.abs((expected - actual) / expected) < 1E-5)
    }

  }
}

