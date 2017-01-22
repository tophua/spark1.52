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
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, Row}

@BeanInfo
case class TokenizerTestData(rawText: String, wantedTokens: Array[String])
/**
 * Tokenizer(分词器)将文本划分为独立个体(通常为单词)
 */
class TokenizerSuite extends SparkFunSuite {

  test("params") {//参数
    ParamsSuite.checkParams(new Tokenizer)
  }
}

class RegexTokenizerSuite extends SparkFunSuite with MLlibTestSparkContext {
  import org.apache.spark.ml.feature.RegexTokenizerSuite._

  test("params") {
    ParamsSuite.checkParams(new RegexTokenizer)
  }

  test("RegexTokenizer") {//正则表达式分解器
    val tokenizer0 = new RegexTokenizer()
    /**
     * RegexTokenizer基于正则表达式提供更多的划分选项。默认情况下,参数“pattern”为划分文本的分隔符
     * 或者,用户可以指定参数“gaps”来指明正则“patten”表示“tokens”而不是分隔符*/
      .setGaps(false).setPattern("\\w+|\\p{Punct}").setInputCol("rawText").setOutputCol("tokens")
    val dataset0 = sqlContext.createDataFrame(Seq(
      TokenizerTestData("Test for tokenization.", Array("Test", "for", "tokenization", ".")),
      TokenizerTestData("Te,st. punct", Array("Te", ",", "st", ".", "punct"))
    ))
    /**
      +--------------------+--------------------+
      |             rawText|        wantedTokens|
      +--------------------+--------------------+
      |Test for tokeniza...|[Test, for, token...|
      |        Te,st. punct|[Te, ,, st, ., pu...|
      +--------------------+--------------------+*/
    dataset0.show()
    testRegexTokenizer(tokenizer0, dataset0)

    val dataset1 = sqlContext.createDataFrame(Seq(
      TokenizerTestData("Test for tokenization.", Array("Test", "for", "tokenization")),
      TokenizerTestData("Te,st. punct", Array("punct"))
    ))
    /**
      +--------------------+--------------------+
      |             rawText|        wantedTokens|
      +--------------------+--------------------+
      |Test for tokeniza...|[Test, for, token...|
      |        Te,st. punct|             [punct]|
      +--------------------+--------------------+*/
    dataset1.show()
    tokenizer0.setMinTokenLength(3)
    
    testRegexTokenizer(tokenizer0, dataset1)

    val tokenizer2 = new RegexTokenizer()
      .setInputCol("rawText")
      .setOutputCol("tokens")
    val dataset2 = sqlContext.createDataFrame(Seq(
      TokenizerTestData("Test for tokenization.", Array("Test", "for", "tokenization.")),
      TokenizerTestData("Te,st.  punct", Array("Te,st.", "punct"))
    ))
    testRegexTokenizer(tokenizer2, dataset2)
  }
}

object RegexTokenizerSuite extends SparkFunSuite {

  def testRegexTokenizer(t: RegexTokenizer, dataset: DataFrame): Unit = {
   //transform()方法将DataFrame转化为另外一个DataFrame的算法
    t.transform(dataset)
      .select("tokens", "wantedTokens")
      .collect()
      .foreach { case Row(tokens, wantedTokens) =>
        assert(tokens === wantedTokens)
      }
  }
}
