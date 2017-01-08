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
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.sql.Row
/**
 * 统计向量
 */
class CountVectorizerSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("params") {//参数
    ParamsSuite.checkParams(new CountVectorizerModel(Array("empty")))
  }

  private def split(s: String): Seq[String] = s.split("\\s+")

  test("CountVectorizerModel common cases") {//计数矢量模型常见的情况
    /**
     res9:= List((0,WrappedArray(a, b, c, d),(4,[0,1,2,3],[1.0,1.0,1.0,1.0])), 
     						 (1,WrappedArray(a, b, b, c, d, a),(4,[0,1,2,3],[2.0,2.0,1.0,1.0])), 
     						 (2,WrappedArray(a),(4,[0],[1.0])),
     						 (3,WrappedArray(""),(4,[],[])), 
     						 (4,WrappedArray(a, notInDict, d),(4,[0,3],[1.0,1.0])))
     */
    val df = sqlContext.createDataFrame(Seq(
      (0, split("a b c d"),
        Vectors.sparse(4, Seq((0, 1.0), (1, 1.0), (2, 1.0), (3, 1.0)))),
      (1, split("a b b c d  a"),
        Vectors.sparse(4, Seq((0, 2.0), (1, 2.0), (2, 1.0), (3, 1.0)))),
      (2, split("a"), Vectors.sparse(4, Seq((0, 1.0)))),
      (3, split(""), Vectors.sparse(4, Seq())), // empty string
      (4, split("a notInDict d"),
        Vectors.sparse(4, Seq((0, 1.0), (3, 1.0))))  // with words not in vocabulary
    )).toDF("id", "words", "expected")

    val cv = new CountVectorizerModel(Array("a", "b", "c", "d")).setInputCol("words").setOutputCol("features")
    /**
      (4,[0,1,2,3],[1.0,1.0,1.0,1.0])~==(4,[0,1,2,3],[1.0,1.0,1.0,1.0])
      (4,[0,1,2,3],[2.0,2.0,1.0,1.0])~==(4,[0,1,2,3],[2.0,2.0,1.0,1.0])
      (4,[0],[1.0])~==(4,[0],[1.0])
      (4,[],[])~==(4,[],[])
      (4,[0,3],[1.0,1.0])~==(4,[0,3],[1.0,1.0])
     */
     //transform()方法将DataFrame转化为另外一个DataFrame的算法
    cv.transform(df).select("features", "expected").collect().foreach {
      case Row(features: Vector, expected: Vector) =>
        println(features+"~=="+expected)
        //assert(features ~== expected absTol 1e-14)
    }
  }

  test("CountVectorizer common cases") {//一般情况下计数矢量
    /**
  	res11:=List((0,WrappedArray(a, b, c, d, e),(5,[0,1,2,3,4],[1.0,1.0,1.0,1.0,1.0])), 
    						(1,WrappedArray(a, a, a, a, a, a),(5,[0],[6.0])), 
    						(2,WrappedArray(c),(5,[2],[1.0])), 
    						(3,WrappedArray(b, b, b, b, b),(5,[1],[5.0])))
     **/
    val df = sqlContext.createDataFrame(Seq(
      (0, split("a b c d e"),Vectors.sparse(5, Seq((0, 1.0), (1, 1.0), (2, 1.0), (3, 1.0), (4, 1.0)))),
      (1, split("a a a a a a"), Vectors.sparse(5, Seq((0, 6.0)))),
      (2, split("c"), Vectors.sparse(5, Seq((2, 1.0)))),
      (3, split("b b b b b"), Vectors.sparse(5, Seq((1, 5.0)))))
    ).toDF("id", "words", "expected")
   //fit()方法将DataFrame转化为一个Transformer的算法
    val cv = new CountVectorizer().setInputCol("words").setOutputCol("features").fit(df)
    //使用的词汇
    assert(cv.vocabulary === Array("a", "b", "c", "d", "e"))

    cv.transform(df).select("features", "expected").collect().foreach {
      case Row(features: Vector, expected: Vector) =>
        //println(features+"|||"+expected)        
        /**
         (5,[0,1,2,3,4],[1.0,1.0,1.0,1.0,1.0])|||(5,[0,1,2,3,4],[1.0,1.0,1.0,1.0,1.0])
         (5,[0],[6.0])|||(5,[0],[6.0])
         (5,[2],[1.0])|||(5,[2],[1.0])
         (5,[1],[5.0])|||(5,[1],[5.0])
         **/
       assert(features ~== expected absTol 1e-14)
    }
  }
  //计数矢量大小和mindf词汇
  test("CountVectorizer vocabSize and minDF") {
    /**
  	res11:=List((0,WrappedArray(a, b, c, d, e),(5,[0,1,2,3,4],[1.0,1.0,1.0,1.0,1.0])), 
    						(1,WrappedArray(a, a, a, a, a, a),(5,[0],[6.0])), 
    						(2,WrappedArray(c),(5,[2],[1.0])), 
    						(3,WrappedArray(b, b, b, b, b),(5,[1],[5.0])))
     **/
    val df = sqlContext.createDataFrame(Seq(
      (0, split("a b c d"), Vectors.sparse(3, Seq((0, 1.0), (1, 1.0)))),
      (1, split("a b c"), Vectors.sparse(3, Seq((0, 1.0), (1, 1.0)))),
      (2, split("a b"), Vectors.sparse(3, Seq((0, 1.0), (1, 1.0)))),
      (3, split("a"), Vectors.sparse(3, Seq((0, 1.0)))))
    ).toDF("id", "words", "expected")
    val cvModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(3)  // limit vocab size to 3 限制单词列表
      .fit(df)//fit()方法将DataFrame转化为一个Transformer的算法
   //使用的词汇      
    assert(cvModel.vocabulary === Array("a", "b", "c"))

    // minDF: ignore terms with count less than 3
    val cvModel2 = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setMinDF(3)
      .fit(df)//fit()方法将DataFrame转化为一个Transformer的算法
    //使用的词汇
    assert(cvModel2.vocabulary === Array("a", "b"))

    cvModel2.transform(df).select("features", "expected").collect().foreach {
      case Row(features: Vector, expected: Vector) =>
        assert(features ~== expected absTol 1e-14)
    }

    // minDF: ignore terms with freq < 0.75
    //minDF:忽略与频率＜0.75
    val cvModel3 = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setMinDF(3.0 / df.count())
      .fit(df)//fit()方法将DataFrame转化为一个Transformer的算法
    assert(cvModel3.vocabulary === Array("a", "b"))
//transform()方法将DataFrame转化为另外一个DataFrame的算法
    cvModel3.transform(df).select("features", "expected").collect().foreach {
      case Row(features: Vector, expected: Vector) =>
        assert(features ~== expected absTol 1e-14)
    }
  }
  //计数矢量抛出异常词汇为空时
  test("CountVectorizer throws exception when vocab is empty") {
    intercept[IllegalArgumentException] {
      val df = sqlContext.createDataFrame(Seq(
        (0, split("a a b b c c")),
        (1, split("aa bb cc")))
      ).toDF("id", "words")
      val cvModel = new CountVectorizer()
        .setInputCol("words")
        .setOutputCol("features")
        .setVocabSize(3) // limit vocab size to 3 限字的大小为3
        .setMinDF(3)
        .fit(df)//fit()方法将DataFrame转化为一个Transformer的算法
    }
  }

  test("CountVectorizerModel with minTF count") {//计数矢量模型与mintf计数
    val df = sqlContext.createDataFrame(Seq(
      (0, split("a a a b b c c c d "), Vectors.sparse(4, Seq((0, 3.0), (2, 3.0)))),
      (1, split("c c c c c c"), Vectors.sparse(4, Seq((2, 6.0)))),
      (2, split("a"), Vectors.sparse(4, Seq())),
      (3, split("e e e e e"), Vectors.sparse(4, Seq())))
    ).toDF("id", "words", "expected")

    // minTF: count
    val cv = new CountVectorizerModel(Array("a", "b", "c", "d"))
      .setInputCol("words")
      .setOutputCol("features")
      .setMinTF(3)
    cv.transform(df).select("features", "expected").collect().foreach {
      case Row(features: Vector, expected: Vector) =>
        assert(features ~== expected absTol 1e-14)
    }
  }
  //计数矢量模型与mintf频率
  test("CountVectorizerModel with minTF freq") {
    val df = sqlContext.createDataFrame(Seq(
      (0, split("a a a b b c c c d "), Vectors.sparse(4, Seq((0, 3.0), (2, 3.0)))),
      (1, split("c c c c c c"), Vectors.sparse(4, Seq((2, 6.0)))),
      (2, split("a"), Vectors.sparse(4, Seq((0, 1.0)))),
      (3, split("e e e e e"), Vectors.sparse(4, Seq())))
    ).toDF("id", "words", "expected")

    // minTF: count
    val cv = new CountVectorizerModel(Array("a", "b", "c", "d"))
      .setInputCol("words")
      .setOutputCol("features")
      .setMinTF(0.3)
      //transform()方法将DataFrame转化为另外一个DataFrame的算法
    cv.transform(df).select("features", "expected").collect().foreach {
      case Row(features: Vector, expected: Vector) =>
        assert(features ~== expected absTol 1e-14)
    }
  }
}
