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
import java.util.Arrays

import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.feature.VectorSlicer

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
// $example off$
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SQLContext, DataFrame}
/**
 * VectorSlicer是一个转换器输入特征向量,输出原始特征向量子集.
 * VectorSlicer接收带有特定索引的向量列,通过对这些索引的值进行筛选得到新的向量集
 */
object VectorSlicerExample {
  def main(args: Array[String]): Unit = {
  val conf = new SparkConf().setAppName("VectorSlicerExample").setMaster("local[4]")
    val sc = new SparkContext(conf)
  
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // $example on$
    val data = Arrays.asList(Row(Vectors.dense(-2.0, 2.3, 0.0)))

    val defaultAttr = NumericAttribute.defaultAttr
    val attrs = Array("f1", "f2", "f3").map(defaultAttr.withName)
    val attrGroup = new AttributeGroup("userFeatures", attrs.asInstanceOf[Array[Attribute]])
    
   // val dataset = sqlContext.createDataFrame(data, StructType(Array(attrGroup.toStructField())))
  //上面系统报错,未调整,主要目的 setIndices方式
  val dataset = sqlContext.createDataFrame(
      Seq((0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0))
    ).toDF("id", "hour", "mobile", "userFeatures", "clicked")
    // VectorSlicer是一个转换器输入特征向量,输出原始特征向量子集.
    val slicer = new VectorSlicer().setInputCol("userFeatures").setOutputCol("features")
    //1,整数索引,setIndices()
    //2,字符串索引代表向量中特征的名字
    slicer.setIndices(Array(1)).setNames(Array("f3"))
    // or slicer.setIndices(Array(1, 2)), or slicer.setNames(Array("f2", "f3"))
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val output = slicer.transform(dataset)
    println(output.select("userFeatures", "features").first())
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println
