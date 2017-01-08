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
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext
  //把字符转换特征哈希值,返回词的频率
/**
 * HashTF从一个文档中计算出给定大小的词频向量。为了将词和向量顺序对应起来，所以使用了哈希。
 * HashingTF使用每个单词对所需向量的长度S取模得出的哈希值，把所有单词映射到一个0到S-1之间的数字上。
 * 由此可以保证生成一个S维的向量。随后当构建好词频向量后，使用IDF来计算逆文档频率,然后将它们与词频相乘计算TF-IDF
 * 
 */
class HashingTFSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("hashing tf on a single doc") {//散列在一个单一的文件
      
    val hashingTF = new HashingTF(1000)
    val doc = "a a b b c d".split(" ")
    val n = hashingTF.numFeatures
    //词的频率
    val termFreqs = Seq(
      (hashingTF.indexOf("a"), 2.0),
      (hashingTF.indexOf("b"), 2.0),
      (hashingTF.indexOf("c"), 1.0),
      (hashingTF.indexOf("d"), 1.0))
    //termFreqs: Seq[(Int, Double)] = List((97,2.0), (98,2.0), (99,1.0), (100,1.0))
    assert(termFreqs.map(_._1).forall(i => i >= 0 && i < n),
      "index must be in range [0, #features)")//索引必须在范围内
    assert(termFreqs.map(_._1).toSet.size === 4, "expecting perfect hashing")//期待完美的哈希
    val expected = Vectors.sparse(n, termFreqs)
    //transform 把每个输入文档映射到一个Vector对象
     //transform()方法将DataFrame转化为另外一个DataFrame的算法
    assert(hashingTF.transform(doc) === expected)
  }

  test("hashing tf on an RDD") {//散列TF在RDD
    val hashingTF = new HashingTF
    val localDocs: Seq[Seq[String]] = Seq(
      "a a b b b c d".split(" "),
      "a b c d a b c".split(" "),
      "c b a c b a a".split(" "))
    val docs = sc.parallelize(localDocs, 2)
     //transform()方法将DataFrame转化为另外一个DataFrame的算法
    assert(hashingTF.transform(docs).collect().toSet === localDocs.map(hashingTF.transform).toSet)
  }
}
