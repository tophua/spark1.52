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
import org.apache.spark.ml.feature.StringIndexer
// $example off$
import org.apache.spark.sql.Row
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SQLContext, DataFrame}

object StringIndexerExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StringIndexerExample").setMaster("local[4]")
    val sc = new SparkContext(conf)
  
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // $example on$
    val df = sqlContext.createDataFrame(
      Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
    ).toDF("id", "category")
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
     * 1)按照 category 出现的频次对其进行序列编码,按升序排序,a出次3次为0,c出现2次为1,b出现1次为2
     * 2)fit方法设计和实现上实际上是采用了模板方法的设计模式,具体会调用实现类的 train方法
     */
    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")

    val indexed = indexer.fit(df).transform(df)
    indexed.show()
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println
