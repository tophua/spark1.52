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

package org.apache.spark.mllib.linalg.distributed

import breeze.linalg.{diag => brzDiag, DenseMatrix => BDM, DenseVector => BDV}

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Matrices, Vectors}
/**
 * 索引行矩阵(IndexedRowMatrix)跟RowMatrix类似,但是有行索引;其底层支撑结构是索引的行组成的RDD
 * 所以每行可以通过索引(long)和局部向量表示
 * IndexedRowMatrix:每一行是一个特征向量,行索引
 */
class IndexedRowMatrixSuite extends SparkFunSuite with MLlibTestSparkContext {

  val m = 4
  val n = 3
  //创建行索引Seq[IndexedRow]
  val data = Seq(
    (0L, Vectors.dense(0.0, 1.0, 2.0)),
    (1L, Vectors.dense(3.0, 4.0, 5.0)),
    (3L, Vectors.dense(9.0, 0.0, 1.0))
  ).map(x => IndexedRow(x._1, x._2))
  var indexedRows: RDD[IndexedRow] = _

  override def beforeAll() {
    super.beforeAll()
    indexedRows = sc.parallelize(data, 2)
  }

  test("size") {
    //索引行矩阵(IndexedRowMatrix)按行分布式存储,有行索引,其底层支撑结构是索引的行组成的RDD,所以每行可以通过索引(long)和局部向量表示
    val mat1 = new IndexedRowMatrix(indexedRows)
    assert(mat1.numRows() === m)//3行 
    assert(mat1.numCols() === n)//4列
    //创建行索引矩阵,5行,0列
    val mat2 = new IndexedRowMatrix(indexedRows, 5, 0)
    assert(mat2.numRows() === 5)
    assert(mat2.numCols() === n)
  }

  test("empty rows") {//空行
    val rows = sc.parallelize(Seq[IndexedRow](), 1)
    //索引行矩阵(IndexedRowMatrix)按行分布式存储,有行索引,其底层支撑结构是索引的行组成的RDD,所以每行可以通过索引(long)和局部向量表示
    val mat = new IndexedRowMatrix(rows)
    intercept[RuntimeException] {
      mat.numRows()
    }
    intercept[RuntimeException] {
      mat.numCols()
    }
  }

  test("toBreeze") {//
    //索引行矩阵(IndexedRowMatrix)按行分布式存储,有行索引,其底层支撑结构是索引的行组成的RDD,所以每行可以通过索引(long)和局部向量表示
    val mat = new IndexedRowMatrix(indexedRows)
    val expected = BDM(
      (0.0, 1.0, 2.0),
      (3.0, 4.0, 5.0),
      (0.0, 0.0, 0.0),
      (9.0, 0.0, 1.0))
    assert(mat.toBreeze() === expected)
  }

  test("toRowMatrix") {//行矩阵
     //索引行矩阵(IndexedRowMatrix)按行分布式存储,有行索引,其底层支撑结构是索引的行组成的RDD,所以每行可以通过索引(long)和局部向量表示
    val idxRowMat = new IndexedRowMatrix(indexedRows)
    //转换行矩阵
    val rowMat = idxRowMat.toRowMatrix()
    assert(rowMat.numCols() === n)//3列
    assert(rowMat.numRows() === 3, "should drop empty rows")
    assert(rowMat.rows.collect().toSeq === data.map(_.vector).toSeq)
  }
//CoordinateMatrix常用于稀疏性比较高的计算中,MatrixEntry是一个 Tuple类型的元素,其中包含行、列和元素值
  test("toCoordinateMatrix") {//协调矩阵
  //索引行矩阵(IndexedRowMatrix)按行分布式存储,有行索引,其底层支撑结构是索引的行组成的RDD,所以每行可以通过索引(long)和局部向量表示
    val idxRowMat = new IndexedRowMatrix(indexedRows)
    //CoordinateMatrix常用于稀疏性比较高的计算中,MatrixEntry是一个 Tuple类型的元素,其中包含行、列和元素值
    val coordMat = idxRowMat.toCoordinateMatrix()
    assert(coordMat.numRows() === m)
    assert(coordMat.numCols() === n)
    assert(coordMat.toBreeze() === idxRowMat.toBreeze())
  }

  test("toBlockMatrix") {//块矩阵
   //索引行矩阵(IndexedRowMatrix)按行分布式存储,有行索引,其底层支撑结构是索引的行组成的RDD,所以每行可以通过索引(long)和局部向量表示
    val idxRowMat = new IndexedRowMatrix(indexedRows)
    //转换块矩阵
    val blockMat = idxRowMat.toBlockMatrix(2, 2)
    assert(blockMat.numRows() === m)//4行
    assert(blockMat.numCols() === n)//3列
    assert(blockMat.toBreeze() === idxRowMat.toBreeze())

    intercept[IllegalArgumentException] {
      idxRowMat.toBlockMatrix(-1, 2)
    }
    /**
    * 分块矩阵(BlockMatrix)是由RDD支撑的分布式矩阵,RDD中的元素为MatrixBlock,
    * MatrixBlock是多个((Int, Int),Matrix)组成的元组,其中(Int,Int)是分块索引,Matriax是指定索引处的子矩阵
    */
    intercept[IllegalArgumentException] {
      idxRowMat.toBlockMatrix(2, 0)
    }
  }

  test("multiply a local matrix") {//乘一个局部矩阵
   //索引行矩阵(IndexedRowMatrix)按行分布式存储,有行索引,其底层支撑结构是索引的行组成的RDD,所以每行可以通过索引(long)和局部向量表示
    val A = new IndexedRowMatrix(indexedRows)
    val B = Matrices.dense(3, 2, Array(0.0, 1.0, 2.0, 3.0, 4.0, 5.0))
    val C = A.multiply(B)
    val localA = A.toBreeze()
    val localC = C.toBreeze()
    val expected = localA * B.toBreeze.asInstanceOf[BDM[Double]]
    assert(localC === expected)
  }

  test("gram") {//格拉姆矩阵
    val A = new IndexedRowMatrix(indexedRows)
    //格拉姆矩阵
    val G = A.computeGramianMatrix()
    val expected = BDM(
      (90.0, 12.0, 24.0),
      (12.0, 17.0, 22.0),
      (24.0, 22.0, 30.0))
    assert(G.toBreeze === expected)
  }

  test("svd") {//奇异值分解
    val A = new IndexedRowMatrix(indexedRows)
    //第一个参数3意味着取top 2个奇异值,第二个参数true意味着计算矩阵U
    val svd = A.computeSVD(n, computeU = true)
    assert(svd.U.isInstanceOf[IndexedRowMatrix])
    val localA = A.toBreeze()
    val U = svd.U.toBreeze()
    val s = svd.s.toBreeze.asInstanceOf[BDV[Double]]
    val V = svd.V.toBreeze.asInstanceOf[BDM[Double]]
    assert(closeToZero(U.t * U - BDM.eye[Double](n)))
    assert(closeToZero(V.t * V - BDM.eye[Double](n)))
    assert(closeToZero(U * brzDiag(s) * V.t - localA))
  }

  test("validate matrix sizes of svd") {//验证SVD矩阵大小
    val k = 2
    val A = new IndexedRowMatrix(indexedRows)
    val svd = A.computeSVD(k, computeU = true)
    assert(svd.U.numRows() === m)
    assert(svd.U.numCols() === k)
    assert(svd.s.size === k)
    assert(svd.V.numRows === n)
    assert(svd.V.numCols === k)
  }

  test("validate k in svd") {//验证K 奇异值
    val A = new IndexedRowMatrix(indexedRows)
    intercept[IllegalArgumentException] {
      A.computeSVD(-1)
    }
  }

  def closeToZero(G: BDM[Double]): Boolean = {
   //math.abs返回数的绝对值
    G.valuesIterator.map(math.abs).sum < 1e-6
  }
}

