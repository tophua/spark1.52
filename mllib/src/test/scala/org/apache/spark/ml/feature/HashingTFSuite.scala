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
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.util.Utils
  //spark 的词频计算是用特征哈希（HashingTF）来计算的。特征哈希是一种处理高维数据的技术,
/** 
 * HashTF从一个文档中计算出给定大小的词频向量。为了将词和向量顺序对应起来，所以使用了哈希。
 * HashingTF使用每个单词对所需向量的长度S取模得出的哈希值，把所有单词映射到一个0到S-1之间的数字上。
 * 由此可以保证生成一个S维的向量。随后当构建好词频向量后，使用IDF来计算逆文档频率,然后将它们与词频相乘计算TF-IDF
 */

class HashingTFSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("params") {
    ParamsSuite.checkParams(new HashingTF)
  }
  test("hashingTF") {
    //List((0,WrappedArray(a, a, b, b, c, d)))
    val df = sqlContext.createDataFrame(Seq(
      (0, "a a b b c d".split(" ").toSeq)
    )).toDF("id", "words")
    val n = 100
    //words:WrappedArray(a, a, b, b, c, d)
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("features").setNumFeatures(n)//将词转化为词频
    val output = hashingTF.transform(df)
    //属性分组
    val attrGroup = AttributeGroup.fromStructField(output.schema("features"))
    //属性100
    require(attrGroup.numAttributes === Some(n))
    //List((100,WrappedArray(a,b,c,d)))
    //features:= (100,[0,97,98,99],[1.0,2.0,2.0,1.0])
    val features = output.select("features").first().getAs[Vector](0)//强制转换Vector
    // 假设 Assume perfect hash on "a", "b", "c", and "d".
    def idx(any: Any): Int = Utils.nonNegativeMod(any.##, n)
    
    println("c:"+idx("c")+"||b:"+idx("d")+"||a:"+idx("a")+"||b:"+idx("b"))
    val expected = Vectors.sparse(n,
      Seq((idx("a"), 2.0), (idx("b"), 2.0), (idx("c"), 1.0), (idx("d"), 1.0)))
                    //0:b|97:a|98:b|99:c
     // expected (100,[0,97,98,99],[1.0,2.0,2.0,1.0]) 
    println(expected)
    assert(features ~== expected absTol 1e-14)
  }
}
