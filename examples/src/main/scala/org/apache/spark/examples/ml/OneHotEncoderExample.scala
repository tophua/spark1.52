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
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
// $example off$
import org.apache.spark.sql.Row
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SQLContext, DataFrame}
/**
 * oneHotEncoder 离散<->连续特征或Label相互转换
 * 独热编码将类别特征(离散的,已经转换为数字编号形式),映射成独热编码。
 * 在诸如Logistic回归这样需要连续数值值作为特征输入的分类器中也可以使用类别(离散)特征
 * 解决了分类器不好处理属性数据的问题
 */
object OneHotEncoderExample {
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("OneHotEncoderExample").setMaster("local[4]")
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
  //onehotencoder前需要转换为string->numerical
    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")
      .fit(df)
    val indexed = indexer.transform(df)
  //对随机分布的类别进行OneHotEncoder，转换后可以当成连续数值输入
    val encoder = new OneHotEncoder()
      .setInputCol("categoryIndex")
      .setOutputCol("categoryVec")
    //注意不需要fit 
    val encoded = encoder.transform(indexed)
    /**
     * 数据会变成稀疏的
     * +---+-------------+
     * | id|  categoryVec|
     * +---+-------------+
     * |  0|(2,[0],[1.0])|
     * |  1|    (2,[],[])|
     * |  2|(2,[1],[1.0])|
     * |  3|(2,[0],[1.0])|
     * |  4|(2,[0],[1.0])|
     * |  5|(2,[1],[1.0])|
     * +---+-------------+
     */
    encoded.select("id", "categoryVec").show()
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println
