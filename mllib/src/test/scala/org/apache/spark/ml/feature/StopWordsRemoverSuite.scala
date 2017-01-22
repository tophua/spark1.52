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
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, Row}

object StopWordsRemoverSuite extends SparkFunSuite {
  def testStopWordsRemover(t: StopWordsRemover, dataset: DataFrame): Unit = {
    t.transform(dataset) //transform()方法将DataFrame转化为另外一个DataFrame的算法
      .select("filtered", "expected")
      .collect()
      .foreach { case Row(tokens, wantedTokens) =>
        assert(tokens === wantedTokens)
    }
  }
}

/**
 * StopWordsRemover的输入为一系列字符串(如分词器输出),输出中删除了所有停用词
 * 停用词为在文档中频繁出现,但未承载太多意义的词语,他们不应该被包含在算法输入中。
 */
class StopWordsRemoverSuite extends SparkFunSuite with MLlibTestSparkContext {
  import StopWordsRemoverSuite._

  test("StopWordsRemover default") {//默认删除停用词
    val remover = new StopWordsRemover()
      .setInputCol("raw")//输入
      .setOutputCol("filtered")//输出
    val dataSet = sqlContext.createDataFrame(Seq(
      (Seq("test", "test"), Seq("test", "test")),
      (Seq("a", "b", "c", "d"), Seq("b", "c", "d")),
      (Seq("a", "the", "an"), Seq()),
      (Seq("A", "The", "AN"), Seq()),
      (Seq(null), Seq(null)),
      (Seq(), Seq())
    )).toDF("raw", "expected")//期望值
    /**
      +------------+------------+
      |         raw|    expected|
      +------------+------------+
      |[test, test]|[test, test]|
      |[a, b, c, d]|   [b, c, d]|
      |[a, the, an]|          []|
      |[A, The, AN]|          []|
      |      [null]|      [null]|
      |          []|          []|
      +------------+------------+*/
    dataSet.show()
    testStopWordsRemover(remover, dataSet)
  }

  test("StopWordsRemover case sensitive") {//删除停用词区分大小写
    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")
      //布尔参数caseSensitive指明是否区分大小写(默认为否)
      .setCaseSensitive(true)
    val dataSet = sqlContext.createDataFrame(Seq(
      (Seq("A"), Seq("A")),
      (Seq("The", "the"), Seq("The"))
    )).toDF("raw", "expected")
    /**
      +----------+--------+
      |       raw|expected|
      +----------+--------+
      |       [A]|     [A]|
      |[The, the]|   [The]|
      +----------+--------+
     */
    dataSet.show()
    testStopWordsRemover(remover, dataSet)
  }

  test("StopWordsRemover with additional words") {//删除停用词 附加词
    val stopWords = StopWords.EnglishStopWords ++ Array("python", "scala")
    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")
      .setStopWords(stopWords)//设置信停用词列表
    val dataSet = sqlContext.createDataFrame(Seq(
      (Seq("python", "scala", "a"), Seq()),
      (Seq("Python", "Scala", "swift"), Seq("swift"))
    )).toDF("raw", "expected")
      /**
        +--------------------+--------+
        |                 raw|expected|
        +--------------------+--------+
        |  [python, scala, a]|      []|
        |[Python, Scala, s...| [swift]|
        +--------------------+--------+*/
    dataSet.show()
    testStopWordsRemover(remover, dataSet)
  }
}
