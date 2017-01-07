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
import org.apache.spark.ml.util.MLTestingUtils
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{Row, SQLContext}
/**
 * MinMaxScaler最大-最小规范化
 * 将所有特征向量线性变换到用户指定最大-最小值之间
 */
class MinMaxScalerSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("MinMaxScaler fit basic case") {//基本最大最小权值
    val sqlContext = new SQLContext(sc)
  /**
   * data:= Array([1.0,0.0,-9.223372036854776E18], [2.0,0.0,0.0], (3,[0,2],[3.0,9.223372036854776E18]), (3,[0],[1.5]))
   */
    val data = Array(
      Vectors.dense(1, 0, Long.MinValue),
      Vectors.dense(2, 0, 0),
      Vectors.sparse(3, Array(0, 2), Array(3, Long.MaxValue)),
      Vectors.sparse(3, Array(0), Array(1.5)))
  /**
   * expected= Array([-5.0,0.0,-5.0], [0.0,0.0,0.0], (3,[0,2],[5.0,5.0]), (3,[0],[-2.5]))
   */
    val expected: Array[Vector] = Array(
      Vectors.dense(-5, 0, -5),
      Vectors.dense(0, 0, 0),
      Vectors.sparse(3, Array(0, 2), Array(5, 5)),
      Vectors.sparse(3, Array(0), Array(-2.5)))

    val df = sqlContext.createDataFrame(data.zip(expected)).toDF("features", "expected")
    val scaler = new MinMaxScaler().setInputCol("features").setOutputCol("scaled")
    //max,min是用户可以重新自定义的范围,将数据线性变换到[-5,5]
    .setMin(-5).setMax(5)

    val model = scaler.fit(df)
    model.transform(df).select("expected", "scaled").collect()
      .foreach { case Row(vector1: Vector, vector2: Vector) =>{
        /**
         * [-5.0,0.0,-5.0]|||[-5.0,0.0,-5.0]
         * [0.0,0.0,0.0]|||[0.0,0.0,0.0]
         * (3,[0,2],[5.0,5.0])|||[5.0,0.0,5.0]
         * (3,[0],[-2.5])|||[-2.5,0.0,0.0]
         */
        println(vector1+"|||"+vector2)
        assert(vector1.equals(vector2), "Transformed vector is different with expected.")
        }
    }

    // copied model must have the same parent.
    MLTestingUtils.checkCopy(model)
  }
  //MinMaxScaler将所有特征向量线性变换到用户指定最大-最小值之间
  test("MinMaxScaler arguments max must be larger than min") {
    withClue("arguments max must be larger than min") {
      intercept[IllegalArgumentException] {
        val scaler = new MinMaxScaler().setMin(10).setMax(0)
        scaler.validateParams()
      }
      intercept[IllegalArgumentException] {
        val scaler = new MinMaxScaler().setMin(0).setMax(0)
        scaler.validateParams()
      }
    }
  }
}
