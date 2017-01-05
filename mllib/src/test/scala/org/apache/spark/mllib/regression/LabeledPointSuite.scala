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

package org.apache.spark.mllib.regression

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.Vectors

class LabeledPointSuite extends SparkFunSuite {
  /**
       标记点是局部向量,向量可以是密集型或者稀疏型,每个向量会关联了一个标签(label)
   MLlib的标记点用于有监督学习算法
      使用double来存储标签值,这样标记点既可以用于回归又可以用于分类。
      在二分类中,标签要么是0要么是1;在多分类中，标签是0, 1, 2, **/
  test("parse labeled points") {//解析标记点
    val points = Seq(
      LabeledPoint(1.0, Vectors.dense(1.0, 0.0)),
      LabeledPoint(0.0, Vectors.sparse(2, Array(1), Array(-1.0))))
      
    points.foreach { p =>{
      /**
       * (1.0,[1.0,0.0])||||(1.0,[1.0,0.0])
			 * (0.0,(2,[1],[-1.0]))||||(0.0,(2,[1],[-1.0]))
       */
        println(p+"||||"+LabeledPoint.parse(p.toString))
        assert(p === LabeledPoint.parse(p.toString))}
    }
  }

  test("parse labeled points with whitespaces") {//解析标记点的空格
    //标记点字符串的解析
    val point = LabeledPoint.parse("(0.0, [1.0, 2.0])")
    assert(point === LabeledPoint(0.0, Vectors.dense(1.0, 2.0)))
  }

  test("parse labeled points with v0.9 format") {//解析标记点的V0.9格式
    //默认密集向量,未指定标记默认1 
    val point = LabeledPoint.parse("1.0,1.0 0.0 -2.0")
    assert(point === LabeledPoint(1.0, Vectors.dense(1.0, 0.0, -2.0)))
  }
}
