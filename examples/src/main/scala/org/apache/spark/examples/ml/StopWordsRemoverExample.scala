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
import org.apache.spark.ml.feature.StopWordsRemover
// $example off$
import org.apache.spark.sql.Row
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SQLContext, DataFrame}

object StopWordsRemoverExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StopWordsRemoverExample").setMaster("local[4]")
    val sc = new SparkContext(conf)
  
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // $example on$
    val remover = new StopWordsRemover()
      .setInputCol("raw")//Ô­Ê¼×Ö¶Î
      .setOutputCol("filtered")//¹ýÂÇºóµÄ×Ö¶Î
    //É¾³ýÍ£¶Ù´Ê
    val dataSet = sqlContext.createDataFrame(Seq(
      (0, Seq("I", "saw", "the", "red", "baloon")),
      (1, Seq("Mary", "had", "a", "little", "lamb"))
    )).toDF("id", "raw")
/**
 * +---+--------------------+--------------------+
 * | id|                 raw|            filtered|
 * +---+--------------------+--------------------+
 * |  0|[I, saw, the, red...|  [saw, red, baloon]|
 * |  1|[Mary, had, a, li...|[Mary, little, lamb]|
 * +---+--------------------+--------------------+
 */
    remover.transform(dataSet).show()
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println
