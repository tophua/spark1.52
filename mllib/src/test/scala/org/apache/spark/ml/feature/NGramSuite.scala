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

import scala.beans.BeanInfo

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, Row}

@BeanInfo
case class NGramTestData(inputTokens: Array[String], wantedNGrams: Array[String])

class NGramSuite extends SparkFunSuite with MLlibTestSparkContext {
  import org.apache.spark.ml.feature.NGramSuite._
/**
 * ngrams该模型基于这样一种假设，第n个词的出现只与前面N-1个词相关，而与其它任何词都不相关,
 * 整句的概率就是各个词出现概率的乘积。
 */
  test("default behavior yields bigram features") {//默认方法产生二元特征
    val nGram = new NGram().setInputCol("inputTokens").setOutputCol("nGrams")
    val dataset = sqlContext.createDataFrame(Seq(
      NGramTestData(//分词
         Array("Test", "for", "ngram", "."),
        //WrappedArray(中国 的, 的 人民, 人民 生活, 生活 .)
        //Array("中国", "的", "人民","生活" ,"."),
        //wantedNGrams
        Array("Test for", "for ngram", "ngram .")
    )))
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    nGram.transform(dataset).select("nGrams", "wantedNGrams").collect().foreach { 
      
      case Row(actualNGrams, wantedNGrams) =>{
        //actualNGrams 实际,wantedNGrams期望值 
        //WrappedArray(Test for, for ngram, ngram .)|||WrappedArray(Test for, for ngram, ngram .)
       // println(actualNGrams+"|||"+wantedNGrams)
        assert(actualNGrams === wantedNGrams)
      }        
     }
    //testNGram(nGram, dataset)
  }
  //
  test("NGramLength=4 yields length 4 n-grams") {
    //设置长度为4
    val nGram = new NGram().setInputCol("inputTokens").setOutputCol("nGrams").setN(4)
    val dataset = sqlContext.createDataFrame(Seq(
      NGramTestData(
        //Array("中国", "的", "人民","生活" ,"."),
        Array("a", "b", "c", "d", "e"),
        Array("a b c d", "b c d e")
      )))
      //transform()方法将DataFrame转化为另外一个DataFrame的算法
      nGram.transform(dataset).select("nGrams", "wantedNGrams").collect().foreach {       
          case Row(actualNGrams, wantedNGrams) =>  println(actualNGrams+"|||"+wantedNGrams)      
      }
    testNGram(nGram, dataset)
  }
  //空输入产生空输出
  test("empty input yields empty output") {
    val nGram = new NGram().setInputCol("inputTokens").setOutputCol("nGrams").setN(4)
    val dataset = sqlContext.createDataFrame(Seq(
      NGramTestData(
        Array(),
        Array()
      )))
    testNGram(nGram, dataset)
  }

  test("input array < n yields empty output") {//输入数组< n产量空输出
    val nGram = new NGram()
      .setInputCol("inputTokens")
      .setOutputCol("nGrams")
      .setN(6)
    val dataset = sqlContext.createDataFrame(Seq(
      NGramTestData(
        Array("a", "b", "c", "d", "e"),
        Array()
      )))
    testNGram(nGram, dataset)
  }
}

object NGramSuite extends SparkFunSuite {

  def testNGram(t: NGram, dataset: DataFrame): Unit = {
  //transform()方法将DataFrame转化为另外一个DataFrame的算法
    t.transform(dataset)
      .select("nGrams", "wantedNGrams")
      .collect()
      .foreach { case Row(actualNGrams, wantedNGrams) =>
        
        assert(actualNGrams === wantedNGrams)
      }
  }
}
