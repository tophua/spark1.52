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
import org.apache.spark.ml.feature.RFormula
// $example off$
import org.apache.spark.sql.Row
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SQLContext, DataFrame}
/**
 * RFormula通过R模型公式来选择列,支持R操作中的部分操作
 * 1. ~分隔目标和对象
 * 2. +合并对象,“+ 0”意味着删除空格
 * 3. :交互（数值相乘,类别二值化）
 * 4. . 除了目标外的全部列
 */
object RFormulaExample {
  def main(args: Array[String]): Unit = {
    
   val conf = new SparkConf().setAppName("RFormulaExample").setMaster("local[4]")
    val sc = new SparkContext(conf)
  
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    // $example on$
    //假设我们有一个DataFrame含有id,country, hour和clicked四列：
    val dataset = sqlContext.createDataFrame(Seq(
      (7, "US", 18, 1.0),
      (8, "CA", 12, 0.0),
      (9, "NZ", 15, 0.0)
    )).toDF("id", "country", "hour", "clicked")
    //如果我们使用RFormula公式clicked ~ country+ hour,
    //则表明我们希望基于country和hour预测clicked
    val formula = new RFormula()
      .setFormula("clicked ~ country + hour")//公式
      .setFeaturesCol("features")//特征列
      .setLabelCol("label")//标签列名
      //fit()方法将DataFrame转化为一个Transformer的算法
      //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val output = formula.fit(dataset).transform(dataset)
    /**
    id | country |hour | clicked | features         | label
    ---|---------|-----|---------|------------------|-------
     7 | "US"    | 18  | 1.0     | [0.0, 0.0, 18.0] | 1.0
     8 | "CA"    | 12  | 0.0     | [0.0, 1.0, 12.0] | 0.0
     9 | "NZ"    | 15  | 0.0     | [1.0, 0.0, 15.0] | 0.0
    */
    output.select("features", "label").show()
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println
