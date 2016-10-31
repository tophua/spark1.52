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

package org.apache.spark.mllib.util

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.TestingUtils._
import org.scalatest.exceptions.TestFailedException

class TestingUtilsSuite extends SparkFunSuite {

  test("Comparing doubles using relative error.") {//使用相对误差比较doubles

    assert(23.1 ~== 23.52 relTol 0.02)
    assert(23.1 ~== 22.74 relTol 0.02)
    assert(23.1 ~= 23.52 relTol 0.02)
    assert(23.1 ~= 22.74 relTol 0.02)
    assert(!(23.1 !~= 23.52 relTol 0.02))
    assert(!(23.1 !~= 22.74 relTol 0.02))

    // Should throw exception with message when test fails.
    //当测试失败时,应抛出异常消息
    intercept[TestFailedException](23.1 !~== 23.52 relTol 0.02)
    intercept[TestFailedException](23.1 !~== 22.74 relTol 0.02)
    intercept[TestFailedException](23.1 ~== 23.63 relTol 0.02)
    intercept[TestFailedException](23.1 ~== 22.34 relTol 0.02)

    assert(23.1 !~== 23.63 relTol 0.02)
    assert(23.1 !~== 22.34 relTol 0.02)
    assert(23.1 !~= 23.63 relTol 0.02)
    assert(23.1 !~= 22.34 relTol 0.02)
    assert(!(23.1 ~= 23.63 relTol 0.02))
    assert(!(23.1 ~= 22.34 relTol 0.02))

    // Comparing against zero should fail the test and throw exception with message
    //对零的比较应该失败的测试和抛出异常的消息
    // saying that the relative error is meaningless in this situation.
    //在这种情况下,相对误差是没有意义的
    intercept[TestFailedException](0.1 ~== 0.0 relTol 0.032)
    intercept[TestFailedException](0.1 ~= 0.0 relTol 0.032)
    intercept[TestFailedException](0.1 !~== 0.0 relTol 0.032)
    intercept[TestFailedException](0.1 !~= 0.0 relTol 0.032)
    intercept[TestFailedException](0.0 ~== 0.1 relTol 0.032)
    intercept[TestFailedException](0.0 ~= 0.1 relTol 0.032)
    intercept[TestFailedException](0.0 !~== 0.1 relTol 0.032)
    intercept[TestFailedException](0.0 !~= 0.1 relTol 0.032)

    // Comparisons of numbers very close to zero.
    //数字比较非常接近于零
    assert(10 * Double.MinPositiveValue ~== 9.5 * Double.MinPositiveValue relTol 0.01)
    assert(10 * Double.MinPositiveValue !~== 11 * Double.MinPositiveValue relTol 0.01)

    assert(-Double.MinPositiveValue ~== 1.18 * -Double.MinPositiveValue relTol 0.012)
    assert(-Double.MinPositiveValue ~== 1.38 * -Double.MinPositiveValue relTol 0.012)
  }

  test("Comparing doubles using absolute error.") {//使用绝对误差比较doubles

    assert(17.8 ~== 17.99 absTol 0.2)
    assert(17.8 ~== 17.61 absTol 0.2)
    assert(17.8 ~= 17.99 absTol 0.2)
    assert(17.8 ~= 17.61 absTol 0.2)
    assert(!(17.8 !~= 17.99 absTol 0.2))
    assert(!(17.8 !~= 17.61 absTol 0.2))

    // Should throw exception with message when test fails.
    //当测试失败时,应抛出异常消息
    intercept[TestFailedException](17.8 !~== 17.99 absTol 0.2)
    intercept[TestFailedException](17.8 !~== 17.61 absTol 0.2)
    intercept[TestFailedException](17.8 ~== 18.01 absTol 0.2)
    intercept[TestFailedException](17.8 ~== 17.59 absTol 0.2)

    assert(17.8 !~== 18.01 absTol 0.2)
    assert(17.8 !~== 17.59 absTol 0.2)
    assert(17.8 !~= 18.01 absTol 0.2)
    assert(17.8 !~= 17.59 absTol 0.2)
    assert(!(17.8 ~= 18.01 absTol 0.2))
    assert(!(17.8 ~= 17.59 absTol 0.2))

    // Comparisons of numbers very close to zero, and both side of zeros
    //比较的数字非常接近于零,和两个侧的零
    assert(
      Double.MinPositiveValue ~== 4 * Double.MinPositiveValue absTol 5 * Double.MinPositiveValue)
    assert(
      Double.MinPositiveValue !~== 6 * Double.MinPositiveValue absTol 5 * Double.MinPositiveValue)

    assert(
      -Double.MinPositiveValue ~== 3 * Double.MinPositiveValue absTol 5 * Double.MinPositiveValue)
    assert(
      Double.MinPositiveValue !~== -4 * Double.MinPositiveValue absTol 5 * Double.MinPositiveValue)
  }

  test("Comparing vectors using relative error.") {//比较向量使用相对误差

    // Comparisons of two dense vectors 两种稠密向量的比较
    assert(Vectors.dense(Array(3.1, 3.5)) ~== Vectors.dense(Array(3.130, 3.534)) relTol 0.01)
    assert(Vectors.dense(Array(3.1, 3.5)) !~== Vectors.dense(Array(3.135, 3.534)) relTol 0.01)
    assert(Vectors.dense(Array(3.1, 3.5)) ~= Vectors.dense(Array(3.130, 3.534)) relTol 0.01)
    assert(Vectors.dense(Array(3.1, 3.5)) !~= Vectors.dense(Array(3.135, 3.534)) relTol 0.01)
    assert(!(Vectors.dense(Array(3.1, 3.5)) !~= Vectors.dense(Array(3.130, 3.534)) relTol 0.01))
    assert(!(Vectors.dense(Array(3.1, 3.5)) ~= Vectors.dense(Array(3.135, 3.534)) relTol 0.01))

    // Should throw exception with message when test fails. 当测试失败时,应抛出异常消息
    intercept[TestFailedException](
      Vectors.dense(Array(3.1, 3.5)) !~== Vectors.dense(Array(3.130, 3.534)) relTol 0.01)

    intercept[TestFailedException](
      Vectors.dense(Array(3.1, 3.5)) ~== Vectors.dense(Array(3.135, 3.534)) relTol 0.01)

    // Comparing against zero should fail the test and throw exception with message
      //对零的比较应该失败的测试和抛出异常的消息
    // saying that the relative error is meaningless in this situation.
      //在这种情况下,相对误差是没有意义的
    intercept[TestFailedException](
      Vectors.dense(Array(3.1, 0.01)) ~== Vectors.dense(Array(3.13, 0.0)) relTol 0.01)

    intercept[TestFailedException](
      Vectors.dense(Array(3.1, 0.01)) ~== Vectors.sparse(2, Array(0), Array(3.13)) relTol 0.01)

    // Comparisons of two sparse vectors
    //两个稀疏向量的比较
    assert(Vectors.dense(Array(3.1, 3.5)) ~==
      Vectors.sparse(2, Array(0, 1), Array(3.130, 3.534)) relTol 0.01)

    assert(Vectors.dense(Array(3.1, 3.5)) !~==
      Vectors.sparse(2, Array(0, 1), Array(3.135, 3.534)) relTol 0.01)
  }

  test("Comparing vectors using absolute error.") {//比较向量使用绝对误差

    // Comparisons of two dense vectors 两种稠密向量的比较
    assert(Vectors.dense(Array(3.1, 3.5, 0.0)) ~==
      Vectors.dense(Array(3.1 + 1E-8, 3.5 + 2E-7, 1E-8)) absTol 1E-6)

    assert(Vectors.dense(Array(3.1, 3.5, 0.0)) !~==
      Vectors.dense(Array(3.1 + 1E-5, 3.5 + 2E-7, 1 + 1E-3)) absTol 1E-6)

    assert(Vectors.dense(Array(3.1, 3.5, 0.0)) ~=
      Vectors.dense(Array(3.1 + 1E-8, 3.5 + 2E-7, 1E-8)) absTol 1E-6)

    assert(Vectors.dense(Array(3.1, 3.5, 0.0)) !~=
      Vectors.dense(Array(3.1 + 1E-5, 3.5 + 2E-7, 1 + 1E-3)) absTol 1E-6)

    assert(!(Vectors.dense(Array(3.1, 3.5, 0.0)) !~=
      Vectors.dense(Array(3.1 + 1E-8, 3.5 + 2E-7, 1E-8)) absTol 1E-6))

    assert(!(Vectors.dense(Array(3.1, 3.5, 0.0)) ~=
      Vectors.dense(Array(3.1 + 1E-5, 3.5 + 2E-7, 1 + 1E-3)) absTol 1E-6))

    // Should throw exception with message when test fails.
      //当测试失败时,应抛出异常消息
    intercept[TestFailedException](Vectors.dense(Array(3.1, 3.5, 0.0)) !~==
      Vectors.dense(Array(3.1 + 1E-8, 3.5 + 2E-7, 1E-8)) absTol 1E-6)

    intercept[TestFailedException](Vectors.dense(Array(3.1, 3.5, 0.0)) ~==
      Vectors.dense(Array(3.1 + 1E-5, 3.5 + 2E-7, 1 + 1E-3)) absTol 1E-6)

    // Comparisons of two sparse vectors 两个稀疏向量的比较
    assert(Vectors.sparse(3, Array(0, 2), Array(3.1, 2.4)) ~==
      Vectors.sparse(3, Array(0, 2), Array(3.1 + 1E-8, 2.4 + 1E-7)) absTol 1E-6)

    assert(Vectors.sparse(3, Array(0, 2), Array(3.1 + 1E-8, 2.4 + 1E-7)) ~==
      Vectors.sparse(3, Array(0, 2), Array(3.1, 2.4)) absTol 1E-6)

    assert(Vectors.sparse(3, Array(0, 2), Array(3.1, 2.4)) !~==
      Vectors.sparse(3, Array(0, 2), Array(3.1 + 1E-3, 2.4)) absTol 1E-6)

    assert(Vectors.sparse(3, Array(0, 2), Array(3.1 + 1E-3, 2.4)) !~==
      Vectors.sparse(3, Array(0, 2), Array(3.1, 2.4)) absTol 1E-6)

    // Comparisons of a dense vector and a sparse vector
    //稠密向量和稀疏向量的比较
    assert(Vectors.sparse(3, Array(0, 2), Array(3.1, 2.4)) ~==
      Vectors.dense(Array(3.1 + 1E-8, 0, 2.4 + 1E-7)) absTol 1E-6)

    assert(Vectors.dense(Array(3.1 + 1E-8, 0, 2.4 + 1E-7)) ~==
      Vectors.sparse(3, Array(0, 2), Array(3.1, 2.4)) absTol 1E-6)

    assert(Vectors.sparse(3, Array(0, 2), Array(3.1, 2.4)) !~==
      Vectors.dense(Array(3.1, 1E-3, 2.4)) absTol 1E-6)
  }
}
