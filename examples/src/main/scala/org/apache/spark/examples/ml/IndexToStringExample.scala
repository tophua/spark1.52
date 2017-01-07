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
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
// $example off$
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SQLContext, DataFrame}
/**
 * StringIndexer对String按频次进行编号,频次最高的转换为0
 */
object IndexToStringExample {
  def main(args: Array[String]) {
   val conf = new SparkConf().setAppName("GradientBoostedTreeClassifierExample").setMaster("local[4]")
    val sc = new SparkContext(conf)
  
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // $example on$
    val df = sqlContext.createDataFrame(Seq(
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, "a"),
      (4, "a"),
      (5, "c")
    )).toDF("id", "category")
    //StringIndexer对String按频次进行编号,频次最高的转换为0
    val indexer = new StringIndexer()
      .setInputCol("category")//Spark默认预测label行
      .setOutputCol("categoryIndex")//转换回来的预测label
      .fit(df)
    val indexed = indexer.transform(df)
    /**
    *+---+--------+-------------+
    *| id|category|categoryIndex|
    *+---+--------+-------------+
    *|  0|       a|          0.0|
    *|  1|       b|          2.0|
    *|  2|       c|          1.0|
    *|  3|       a|          0.0|
    *|  4|       a|          0.0|
    *|  5|       c|          1.0|
    *+---+--------+-------------+
     */
    indexed.select("id","category", "categoryIndex").show()
    val converter = new IndexToString()
      .setInputCol("categoryIndex")
      .setOutputCol("originalCategory")

    val converted = converter.transform(indexed)
    /**
     *+---+----------------+
      | id|originalCategory|
      +---+----------------+
      |  0|               a|
      |  1|               b|
      |  2|               c|
      |  3|               a|
      |  4|               a|
      |  5|               c|
      +---+----------------+*/
    converted.select("id", "originalCategory").show()
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println
