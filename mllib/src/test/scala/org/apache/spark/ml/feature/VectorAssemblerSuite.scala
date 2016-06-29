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

import org.apache.spark.SparkException
import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.attribute.NumericAttribute
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
/**
 * 使用 VectorAssembler 从源数据中提取特征指标数据, 这是一个比较典型且通用的步骤,
 * 因为我们的原始数据集里,因为我们的原始数据集里，经常会包含一些非指标数据，如 ID，Description 等
 */
class VectorAssemblerSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("params") {
    ParamsSuite.checkParams(new VectorAssembler)
  }

  test("assemble") {//集合
    import org.apache.spark.ml.feature.VectorAssembler.assemble
    assert(assemble(0.0) === Vectors.sparse(1, Array.empty, Array.empty))
    assert(assemble(0.0, 1.0) === Vectors.sparse(2, Array(1), Array(1.0)))
    val dv = Vectors.dense(2.0, 0.0)
    assert(assemble(0.0, dv, 1.0) === Vectors.sparse(4, Array(1, 3), Array(2.0, 1.0)))
    val sv = Vectors.sparse(2, Array(0, 1), Array(3.0, 4.0))
    assert(assemble(0.0, dv, 1.0, sv) ===
      Vectors.sparse(6, Array(1, 3, 4, 5), Array(2.0, 1.0, 3.0, 4.0)))
    for (v <- Seq(1, "a", null)) {
      intercept[SparkException](assemble(v))
      intercept[SparkException](assemble(1.0, v))
    }
  }

  test("assemble should compress vectors") {
    import org.apache.spark.ml.feature.VectorAssembler.assemble
    val v1 = assemble(0.0, 0.0, 0.0, Vectors.dense(4.0))
    assert(v1.isInstanceOf[SparseVector])
    val v2 = assemble(1.0, 2.0, 3.0, Vectors.sparse(1, Array(0), Array(4.0)))
    assert(v2.isInstanceOf[DenseVector])
  }

  test("VectorAssembler") {
    //Seq:List((0,0.0,[1.0,2.0],a,(2,[1],[3.0]),10))
    val df = sqlContext.createDataFrame(Seq(
      (0, 0.0, Vectors.dense(1.0, 2.0), "a", Vectors.sparse(2, Array(1), Array(3.0)), 10L)
    )).toDF("id", "x", "y", "name", "z", "n")
    df.registerTempTable("src")
    /**
     +---+---+---------+-------------+---+----+
      | id|  x|        y|            z|  n|name|
      +---+---+---------+-------------+---+----+
      |  0|0.0|[1.0,2.0]|(2,[1],[3.0])| 10|   a|
      +---+---+---------+-------------+---+----+
     */
      val queryCaseWhen = sqlContext.sql("select id,x,y,z,n,name from src ").show()
    val a=Seq(
      (0, 0.0, Vectors.dense(1.0, 2.0), "a", Vectors.sparse(2, Array(1), Array(3.0)), 10L)
    )
    println("Seq:"+a)
    val assembler = new VectorAssembler()
      .setInputCols(Array("x", "y", "z", "n"))//源数据 DataFrame 中存储文本词数组列的名称
      .setOutputCol("features")//经过处理的数值型特征向量存储列名称
    //res8: Array[org.apache.spark.sql.Row] = Array([[0.0,1.0,2.0,0.0,3.0,10.0]])
    assembler.transform(df).select("features").collect().foreach {
      case Row(v: Vector) =>
        println(">>>>>>>>>>>>"+v)
        //[0.0,1.0,2.0,0.0,3.0,10.0]
        assert(v === Vectors.sparse(6, Array(1, 2, 4, 5), Array(1.0, 2.0, 3.0, 10.0)))
    }
  }

  test("ML attributes") {
    val browser = NominalAttribute.defaultAttr.withValues("chrome", "firefox", "safari")
    val hour = NumericAttribute.defaultAttr.withMin(0.0).withMax(24.0)
    val user = new AttributeGroup("user", Array(
      NominalAttribute.defaultAttr.withName("gender").withValues("male", "female"),
      NumericAttribute.defaultAttr.withName("salary")))
    val row = (1.0, 0.5, 1, Vectors.dense(1.0, 1000.0), Vectors.sparse(2, Array(1), Array(2.0)))
    val df = sqlContext.createDataFrame(Seq(row)).toDF("browser", "hour", "count", "user", "ad")
      .select(
        col("browser").as("browser", browser.toMetadata()),
        col("hour").as("hour", hour.toMetadata()),
        col("count"), // "count" is an integer column without ML attribute
        col("user").as("user", user.toMetadata()),
        col("ad")) // "ad" is a vector column without ML attribute
    val assembler = new VectorAssembler()
      .setInputCols(Array("browser", "hour", "count", "user", "ad"))
      .setOutputCol("features")
    val output = assembler.transform(df)
    val schema = output.schema
    val features = AttributeGroup.fromStructField(schema("features"))
    assert(features.size === 7)
    val browserOut = features.getAttr(0)
    assert(browserOut === browser.withIndex(0).withName("browser"))
    val hourOut = features.getAttr(1)
    assert(hourOut === hour.withIndex(1).withName("hour"))
    val countOut = features.getAttr(2)
    assert(countOut === NumericAttribute.defaultAttr.withName("count").withIndex(2))
    val userGenderOut = features.getAttr(3)
    assert(userGenderOut === user.getAttr("gender").withName("user_gender").withIndex(3))
    val userSalaryOut = features.getAttr(4)
    assert(userSalaryOut === user.getAttr("salary").withName("user_salary").withIndex(4))
    assert(features.getAttr(5) === NumericAttribute.defaultAttr.withIndex(5))
    assert(features.getAttr(6) === NumericAttribute.defaultAttr.withIndex(6))
  }
}
