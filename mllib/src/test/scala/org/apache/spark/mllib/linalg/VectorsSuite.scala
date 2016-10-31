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

package org.apache.spark.mllib.linalg

import scala.util.Random

import breeze.linalg.{DenseMatrix => BDM, squaredDistance => breezeSquaredDistance}

import org.apache.spark.{Logging, SparkException, SparkFunSuite}
import org.apache.spark.mllib.util.TestingUtils._
/**
 * 向量
 */
class VectorsSuite extends SparkFunSuite with Logging {

  val arr = Array(0.1, 0.0, 0.3, 0.4)
  //长度
  val n = 4
  //索引
  val indices = Array(0, 2, 3)
  //值
  val values = Array(0.1, 0.3, 0.4)
  //密集向量(dense vector)使用double数组表示元素值
  test("dense vector construction with varargs") {//可变参数构建密集向量
    //密集向量
    val vec = Vectors.dense(arr).asInstanceOf[DenseVector]
    assert(vec.size === arr.length)
    assert(vec.values.eq(arr))
  }
  
  test("dense vector construction from a double array") {//双数组构造密集矢量
   //密集向量
   val vec = Vectors.dense(arr).asInstanceOf[DenseVector]
    assert(vec.size === arr.length)
    assert(vec.values.eq(arr))
  }

  test("sparse vector construction") {
    //稀疏向量 第一参数4表示此向量的长度，第二个参数Array(0,2,3)表示的索引，第三个参数表示值Array(0.1, 0.3, 0.4)
    //与前面的Array(0, 2, 3)是相互对应的，表示第0个位置的值为1.0，第2个位置的值为3,即第2位置为0
    val vec = Vectors.sparse(n, indices, values).asInstanceOf[SparseVector]
    assert(vec.size === n)//稀疏矩阵长度 
    assert(vec.indices.eq(indices))//稀疏矩阵索引
    assert(vec.values.eq(values))//稀疏矩阵值
  }

  test("sparse vector construction with unordered elements") {//无序元素构建稀疏向量
    //构建一个没有索引的元素,reverse: Array[(Int, Double)] = Array((3,0.4), (2,0.3), (0,0.1))
    val vec = Vectors.sparse(n, indices.zip(values).reverse).asInstanceOf[SparseVector] 
    assert(vec.size === n)
    assert(vec.indices === indices)
    assert(vec.values === values)
  }
  //构造具有不匹配索引/值数组的稀疏向量
  test("sparse vector construction with mismatched indices/values array") {
    intercept[IllegalArgumentException] {
      Vectors.sparse(4, Array(1, 2, 3), Array(3.0, 5.0, 7.0, 9.0))
    }
    intercept[IllegalArgumentException] {
      Vectors.sparse(4, Array(1, 2, 3), Array(3.0, 5.0))
    }
  }
  //构造具有太多索引和大小的稀疏向量
  test("sparse vector construction with too many indices vs size") {
    intercept[IllegalArgumentException] {
      Vectors.sparse(3, Array(1, 2, 3, 4), Array(3.0, 5.0, 7.0, 9.0))
    }
  }

  test("dense to array") {//密集矩阵转换数组
    val vec = Vectors.dense(arr).asInstanceOf[DenseVector]
    assert(vec.toArray.eq(arr))
  }

  test("dense argmax") {//密集矩阵最大元素位置
    val vec = Vectors.dense(Array.empty[Double]).asInstanceOf[DenseVector]
    assert(vec.argmax === -1)//找出最大元素位置

    val vec2 = Vectors.dense(arr).asInstanceOf[DenseVector]
    assert(vec2.argmax === 3)//找出最大元素位置

    val vec3 = Vectors.dense(Array(-1.0, 0.0, -2.0, 1.0)).asInstanceOf[DenseVector]
    assert(vec3.argmax === 3)//找出最大元素位置
  }

  test("sparse to array") {//稀疏矩阵转换数组
    val vec = Vectors.sparse(n, indices, values).asInstanceOf[SparseVector]
    assert(vec.toArray === arr)
  }

  test("sparse argmax") {//稀疏矩阵最大元素位置
    val vec = Vectors.sparse(0, Array.empty[Int], Array.empty[Double]).asInstanceOf[SparseVector]
    assert(vec.argmax === -1)//找出最大元素位置

    val vec2 = Vectors.sparse(n, indices, values).asInstanceOf[SparseVector]
    assert(vec2.argmax === 3)//找出最大元素位置

    val vec3 = Vectors.sparse(5, Array(2, 3, 4), Array(1.0, 0.0, -.7))
    assert(vec3.argmax === 2)//找出最大元素位置

    // check for case that sparse vector is created with
    //检查稀疏向量的情况下,只有负值创建
    // only negative values {0.0, 0.0,-1.0, -0.7, 0.0}
    val vec4 = Vectors.sparse(5, Array(2, 3), Array(-1.0, -.7))
    assert(vec4.argmax === 0)//找出最大元素位置

    val vec5 = Vectors.sparse(11, Array(0, 3, 10), Array(-1.0, -.7, 0.0))
    assert(vec5.argmax === 1)//找出最大元素位置

    val vec6 = Vectors.sparse(11, Array(0, 1, 2), Array(-1.0, -.7, 0.0))
    assert(vec6.argmax === 2)//找出最大元素位置

    val vec7 = Vectors.sparse(5, Array(0, 1, 3), Array(-1.0, 0.0, -.7))
    assert(vec7.argmax === 1)//找出最大元素位置

    val vec8 = Vectors.sparse(5, Array(1, 2), Array(0.0, -1.0))
    assert(vec8.argmax === 0)//找出最大元素位置
  }

  test("vector equals") {//向量等于
    val dv1 = Vectors.dense(arr.clone())
    val dv2 = Vectors.dense(arr.clone())
    val sv1 = Vectors.sparse(n, indices.clone(), values.clone())
    val sv2 = Vectors.sparse(n, indices.clone(), values.clone())

    val vectors = Seq(dv1, dv2, sv1, sv2)

    for (v <- vectors; u <- vectors) {
      assert(v === u)
      assert(v.## === u.##)
    }

    val another = Vectors.dense(0.1, 0.2, 0.3, 0.4)

    for (v <- vectors) {
      assert(v != another)
      assert(v.## != another.##)
    }
  }

  test("vectors equals with explicit 0") {//向量等于显式0
    val dv1 = Vectors.dense(Array(0, 0.9, 0, 0.8, 0))
    val sv1 = Vectors.sparse(5, Array(1, 3), Array(0.9, 0.8))
    val sv2 = Vectors.sparse(5, Array(0, 1, 2, 3, 4), Array(0, 0.9, 0, 0.8, 0))

    val vectors = Seq(dv1, sv1, sv2)
    for (v <- vectors; u <- vectors) {
      assert(v === u)
      assert(v.## === u.##)
    }

    val another = Vectors.sparse(5, Array(0, 1, 3), Array(0, 0.9, 0.2))
    for (v <- vectors) {
      assert(v != another)
      assert(v.## != another.##)
    }
  }

  test("indexing dense vectors") {//稠密向量索引
    val vec = Vectors.dense(1.0, 2.0, 3.0, 4.0)
    assert(vec(0) === 1.0)
    assert(vec(3) === 4.0)
  }

  test("indexing sparse vectors") {//索引稀疏向量
    //稀疏向量 第一参数7表示此向量的长度，第二个参数Array(0, 2, 4, 6)表示的索引，第三个参数Array(1.0, 2.0, 3.0, 4.0)
    //与前面的Array(0, 2, 4, 6)是相互对应的，表示第0个位置的值为1.0，第2个位置的值为2,第4个位置的值为3,第6个位置的值为4
    val vec = Vectors.sparse(7, Array(0, 2, 4, 6), Array(1.0, 2.0, 3.0, 4.0))
    assert(vec(0) === 1.0)
    assert(vec(1) === 0.0)//第一个Array没有1值,数据为0
    assert(vec(2) === 2.0)
    assert(vec(3) === 0.0)//第一个Array没有3值,数据为0
    assert(vec(6) === 4.0)
    val vec2 = Vectors.sparse(8, Array(0, 2, 4, 6), Array(1.0, 2.0, 3.0, 4.0))
    assert(vec2(6) === 4.0)
    assert(vec2(7) === 0.0)
  }

  test("parse vectors") {//解析向量
    val vectors = Seq(
      Vectors.dense(Array.empty[Double]),
      Vectors.dense(1.0),
      Vectors.dense(1.0E6, 0.0, -2.0e-7),
      Vectors.sparse(0, Array.empty[Int], Array.empty[Double]),
      Vectors.sparse(1, Array(0), Array(1.0)),
      Vectors.sparse(3, Array(0, 2), Array(1.0, -2.0)))
    vectors.foreach { v =>
      val v1 = Vectors.parse(v.toString)
      assert(v.getClass === v1.getClass)
      assert(v === v1)
    }

    val malformatted = Seq("1", "[1,,]", "[1,2b]", "(1,[1,2])", "([1],[2.0,1.0])")
    malformatted.foreach { s =>
      intercept[SparkException] {
        Vectors.parse(s)
        logInfo(s"Didn't detect malformatted string $s.")
      }
    }
  }

  test("zeros") {//零点
    assert(Vectors.zeros(3) === Vectors.dense(0.0, 0.0, 0.0))
  }

  test("Vector.copy") {//矢量复制
    val sv = Vectors.sparse(4, Array(0, 2), Array(1.0, 2.0))
    val svCopy = sv.copy
    (sv, svCopy) match {
      case (sv: SparseVector, svCopy: SparseVector) =>
        assert(sv.size === svCopy.size)
        assert(sv.indices === svCopy.indices)
        assert(sv.values === svCopy.values)
        assert(!sv.indices.eq(svCopy.indices))
        assert(!sv.values.eq(svCopy.values))
      case _ =>
        throw new RuntimeException(s"copy returned ${svCopy.getClass} on ${sv.getClass}.")
    }

    val dv = Vectors.dense(1.0, 0.0, 2.0)
    val dvCopy = dv.copy
    (dv, dvCopy) match {
      case (dv: DenseVector, dvCopy: DenseVector) =>
        assert(dv.size === dvCopy.size)
        assert(dv.values === dvCopy.values)
        assert(!dv.values.eq(dvCopy.values))
      case _ =>
        throw new RuntimeException(s"copy returned ${dvCopy.getClass} on ${dv.getClass}.")
    }
  }

  test("VectorUDT") {
    val dv0 = Vectors.dense(Array.empty[Double])
    val dv1 = Vectors.dense(1.0, 2.0)
    val sv0 = Vectors.sparse(2, Array.empty, Array.empty)
    val sv1 = Vectors.sparse(2, Array(1), Array(2.0))
    val udt = new VectorUDT()
    for (v <- Seq(dv0, dv1, sv0, sv1)) {
      assert(v === udt.deserialize(udt.serialize(v)))
    }
    assert(udt.typeName == "vector")
    assert(udt.simpleString == "vector")
  }

  test("fromBreeze") {//来自Breeze
    val x = BDM.zeros[Double](10, 10)
    val v = Vectors.fromBreeze(x(::, 0))
    assert(v.size === x.rows)
  }

  test("sqdist") {//平方距离
    val random = new Random()
    for (m <- 1 until 1000 by 100) {
      val nnz = random.nextInt(m)

      val indices1 = random.shuffle(0 to m - 1).slice(0, nnz).sorted.toArray
      val values1 = Array.fill(nnz)(random.nextDouble)
      val sparseVector1 = Vectors.sparse(m, indices1, values1)

      val indices2 = random.shuffle(0 to m - 1).slice(0, nnz).sorted.toArray
      val values2 = Array.fill(nnz)(random.nextDouble)
      val sparseVector2 = Vectors.sparse(m, indices2, values2)

      val denseVector1 = Vectors.dense(sparseVector1.toArray)
      val denseVector2 = Vectors.dense(sparseVector2.toArray)

      val squaredDist = breezeSquaredDistance(sparseVector1.toBreeze, sparseVector2.toBreeze)

      // SparseVector vs. SparseVector
      assert(Vectors.sqdist(sparseVector1, sparseVector2) ~== squaredDist relTol 1E-8)
      // DenseVector  vs. SparseVector
      assert(Vectors.sqdist(denseVector1, sparseVector2) ~== squaredDist relTol 1E-8)
      // DenseVector  vs. DenseVector
      assert(Vectors.sqdist(denseVector1, denseVector2) ~== squaredDist relTol 1E-8)
    }
  }

  test("foreachActive") {//迭代活动
    val dv = Vectors.dense(0.0, 1.2, 3.1, 0.0)
    //4表示此向量的长度，后面的比较直观，Seq里面每一对都是(索引，值）的形式。
    val sv = Vectors.sparse(4, Seq((1, 1.2), (2, 3.1), (3, 0.0)))

    val dvMap = scala.collection.mutable.Map[Int, Double]()
    dv.foreachActive { (index, value) =>
      dvMap.put(index, value)
    }
    assert(dvMap.size === 4)
    assert(dvMap.get(0) === Some(0.0))
    assert(dvMap.get(1) === Some(1.2))
    assert(dvMap.get(2) === Some(3.1))
    assert(dvMap.get(3) === Some(0.0))
    //4表示此向量的长度，后面的比较直观，Seq里面每一对都是(索引，值）的形式。
    val svMap = scala.collection.mutable.Map[Int, Double]()
    sv.foreachActive { (index, value) =>
      svMap.put(index, value)
    }
    assert(svMap.size === 3)
    assert(svMap.get(1) === Some(1.2))
    assert(svMap.get(2) === Some(3.1))
    assert(svMap.get(3) === Some(0.0))
  }

  test("vector p-norm") {//向量范数
    val dv = Vectors.dense(0.0, -1.2, 3.1, 0.0, -4.5, 1.9)
    val sv = Vectors.sparse(6, Seq((1, -1.2), (2, 3.1), (3, 0.0), (4, -4.5), (5, 1.9)))
    //矩阵范数 norm 10.700000000000001,即所有元素相加
    assert(Vectors.norm(dv, 1.0) ~== dv.toArray.foldLeft(0.0)((a, v) =>
      {
        /**
         * 0.0     0.0
         * 0.0     1.2
         * 1.2     3.1
         * 4.3     0.0
         * 4.3     4.5
         * 8.8     1.9
         */
      println(a+"\t"+ math.abs(v))
      a + math.abs(v)
      }) relTol 1E-8)
    assert(Vectors.norm(sv, 1.0) ~== sv.toArray.foldLeft(0.0)((a, v) =>
      a + math.abs(v)) relTol 1E-8)
    //正平方根
    assert(Vectors.norm(dv, 2.0) ~== math.sqrt(dv.toArray.foldLeft(0.0)((a, v) =>
      a + v * v)) relTol 1E-8)
    assert(Vectors.norm(sv, 2.0) ~== math.sqrt(sv.toArray.foldLeft(0.0)((a, v) =>
      a + v * v)) relTol 1E-8)

    assert(Vectors.norm(dv, Double.PositiveInfinity) ~== dv.toArray.map(math.abs).max relTol 1E-8)
    assert(Vectors.norm(sv, Double.PositiveInfinity) ~== sv.toArray.map(math.abs).max relTol 1E-8)

    assert(Vectors.norm(dv, 3.7) ~== math.pow(dv.toArray.foldLeft(0.0)((a, v) =>
      a + math.pow(math.abs(v), 3.7)), 1.0 / 3.7) relTol 1E-8)
    assert(Vectors.norm(sv, 3.7) ~== math.pow(sv.toArray.foldLeft(0.0)((a, v) =>
      a + math.pow(math.abs(v), 3.7)), 1.0 / 3.7) relTol 1E-8)
  }

  test("Vector numActive and numNonzeros") {//
    val dv = Vectors.dense(0.0, 2.0, 3.0, 0.0)
    assert(dv.numActives === 4)//最大数据
    assert(dv.numNonzeros === 2)//非零值
   //res4: org.apache.spark.mllib.linalg.DenseVector = [0.0,2.0,3.0,0.0]
    val sv = Vectors.sparse(4, Array(0, 1, 2), Array(0.0, 2.0, 3.0))
    sv.toDense
    assert(sv.numActives === 3)
    assert(sv.numNonzeros === 2)
  }

  test("Vector toSparse and toDense") {//向量转换稀疏矩阵和密集矩阵
    val dv0 = Vectors.dense(0.0, 2.0, 3.0, 0.0)
    assert(dv0.toDense === dv0)
    val dv0s = dv0.toSparse
    assert(dv0s.numActives === 2)
    assert(dv0s === dv0)

    val sv0 = Vectors.sparse(4, Array(0, 1, 2), Array(0.0, 2.0, 3.0))
    assert(sv0.toDense === sv0)
    val sv0s = sv0.toSparse
    assert(sv0s.numActives === 2)
    assert(sv0s === sv0)
  }

  test("Vector.compressed") {//向量压缩
    val dv0 = Vectors.dense(1.0, 2.0, 3.0, 0.0)
    val dv0c = dv0.compressed.asInstanceOf[DenseVector]
    assert(dv0c === dv0)

    val dv1 = Vectors.dense(0.0, 2.0, 0.0, 0.0)
    val dv1c = dv1.compressed.asInstanceOf[SparseVector]
    assert(dv1 === dv1c)
    assert(dv1c.numActives === 1)

    val sv0 = Vectors.sparse(4, Array(1, 2), Array(2.0, 0.0))
    val sv0c = sv0.compressed.asInstanceOf[SparseVector]
    assert(sv0 === sv0c)
    assert(sv0c.numActives === 1)

    val sv1 = Vectors.sparse(4, Array(0, 1, 2), Array(1.0, 2.0, 3.0))
    val sv1c = sv1.compressed.asInstanceOf[DenseVector]
    assert(sv1 === sv1c)
  }

  test("SparseVector.slice") {//稀疏向量切片
    val v = new SparseVector(5, Array(1, 2, 4), Array(1.1, 2.2, 4.4))
    assert(v.slice(Array(0, 2)) === new SparseVector(2, Array(1), Array(2.2)))
    assert(v.slice(Array(2, 0)) === new SparseVector(2, Array(0), Array(2.2)))
    assert(v.slice(Array(2, 0, 3, 4)) === new SparseVector(4, Array(0, 3), Array(2.2, 4.4)))
  }
}
