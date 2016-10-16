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

import java.io.File

import scala.io.Source

import breeze.linalg.{squaredDistance => breezeSquaredDistance}
import com.google.common.base.Charsets
import com.google.common.io.Files

import org.apache.spark.SparkException
import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils._
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.util.Utils

class MLUtilsSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("epsilon computation") {//epsilon表示大于零的最小正Double值计算
    assert(1.0 + EPSILON > 1.0, s"EPSILON is too small: $EPSILON.")
    assert(1.0 + EPSILON / 2.0 === 1.0, s"EPSILON is too big: $EPSILON.")
  }

  test("fast squared distance") {//快速平方距离
    //pow 第一个参数的值提高到第二个参数的幂
    val a = (30 to 0 by -1).map(math.pow(2.0, _)).toArray
    val n = a.length
    val v1 = Vectors.dense(a)
    val norm1 = Vectors.norm(v1, 2.0)
    val precision = 1e-6
    for (m <- 0 until n) {
      val indices = (0 to m).toArray
      val values = indices.map(i => a(i))
      val v2 = Vectors.sparse(n, indices, values)
      val norm2 = Vectors.norm(v2, 2.0)
      val v3 = Vectors.sparse(n, indices, indices.map(i => a(i) + 0.5))
      val norm3 = Vectors.norm(v3, 2.0)
      val squaredDist = breezeSquaredDistance(v1.toBreeze, v2.toBreeze)
      //是一种快速计算向量距离的方法,主要用于KMeans聚合算法中,先计算一个精度
      val fastSquaredDist1 = fastSquaredDistance(v1, norm1, v2, norm2, precision)
      assert((fastSquaredDist1 - squaredDist) <= precision * squaredDist, s"failed with m = $m")
      val fastSquaredDist2 =
        fastSquaredDistance(v1, norm1, Vectors.dense(v2.toArray), norm2, precision)
      assert((fastSquaredDist2 - squaredDist) <= precision * squaredDist, s"failed with m = $m")
      val squaredDist2 = breezeSquaredDistance(v2.toBreeze, v3.toBreeze)
      val fastSquaredDist3 =
        fastSquaredDistance(v2, norm2, v3, norm3, precision)
      assert((fastSquaredDist3 - squaredDist2) <= precision * squaredDist2, s"failed with m = $m")
      if (m > 10) {
        val v4 = Vectors.sparse(n, indices.slice(0, m - 10),
          indices.map(i => a(i) + 0.5).slice(0, m - 10))
        val norm4 = Vectors.norm(v4, 2.0)
        val squaredDist = breezeSquaredDistance(v2.toBreeze, v4.toBreeze)
        val fastSquaredDist =
          fastSquaredDistance(v2, norm2, v4, norm4, precision)
        assert((fastSquaredDist - squaredDist) <= precision * squaredDist, s"failed with m = $m")
      }
    }
  }

  test("loadLibSVMFile") {//加载库支持向量机文件
    //使用三个引号来进行多行字符引用
    val lines =
      """
        |1 1:1.0 3:2.0 5:3.0
        |0
        |0 2:4.0 4:5.0 6:6.0
      """.stripMargin//stripMargin默认是“|”作为出来连接符，在多行换行的行头前面加一个“|”符号即可
    val tempDir = Utils.createTempDir()
    val file = new File(tempDir.getPath, "part-00000")
    Files.write(lines, file, Charsets.US_ASCII)
    val path = tempDir.toURI.toString
    //读取LIBSVM格式的训练数据,每行表示一个标记的稀疏特征向量
    val pointsWithNumFeatures = loadLibSVMFile(sc, path, 6).collect()
    //读取LIBSVM格式的训练数据,每行表示一个标记的稀疏特征向量
    val pointsWithoutNumFeatures = loadLibSVMFile(sc, path).collect()

    for (points <- Seq(pointsWithNumFeatures, pointsWithoutNumFeatures)) {
      assert(points.length === 3)
      assert(points(0).label === 1.0)
      assert(points(0).features === Vectors.sparse(6, Seq((0, 1.0), (2, 2.0), (4, 3.0))))
      assert(points(1).label == 0.0)
      assert(points(1).features == Vectors.sparse(6, Seq()))
      assert(points(2).label === 0.0)
      assert(points(2).features === Vectors.sparse(6, Seq((1, 4.0), (3, 5.0), (5, 6.0))))
    }
    //读取LIBSVM格式的训练数据,每行表示一个标记的稀疏特征向量
    val multiclassPoints = loadLibSVMFile(sc, path).collect()
    //字符串使用空格分隔，索引从0开始，以递增的训练排列。导入系统后，特征索引自动转为从0开始索引
    assert(multiclassPoints.length === 3)
    assert(multiclassPoints(0).label === 1.0)
    assert(multiclassPoints(1).label === 0.0)
    assert(multiclassPoints(2).label === 0.0)

    Utils.deleteRecursively(tempDir)
  }
  //加载库支持向量机文件,在索引为零的情况时抛出非法参数异常
  test("loadLibSVMFile throws IllegalArgumentException when indices is zero-based") {
    val lines =
      """
        |0
        |0 0:4.0 4:5.0 6:6.0
      """.stripMargin
    val tempDir = Utils.createTempDir()
    val file = new File(tempDir.getPath, "part-00000")
    Files.write(lines, file, Charsets.US_ASCII)
    val path = tempDir.toURI.toString

    intercept[SparkException] {
      //loadLibSVMFile加载指定LIBSVM格式文件方法
      loadLibSVMFile(sc, path).collect()
    }
    Utils.deleteRecursively(tempDir)
  }
 //加载库支持向量机文件,在索引没有升序排序情况时,抛出非法参数异常
  test("loadLibSVMFile throws IllegalArgumentException when indices is not in ascending order") {
    val lines =
      """
        |0
        |0 3:4.0 2:5.0 6:6.0
      """.stripMargin
    val tempDir = Utils.createTempDir()
    val file = new File(tempDir.getPath, "part-00000")
    Files.write(lines, file, Charsets.US_ASCII)
    val path = tempDir.toURI.toString

    intercept[SparkException] {
      loadLibSVMFile(sc, path).collect()
    }
    Utils.deleteRecursively(tempDir)
  }

  test("saveAsLibSVMFile") {//保存为支持向量机文件
    val examples = sc.parallelize(Seq(
      LabeledPoint(1.1, Vectors.sparse(3, Seq((0, 1.23), (2, 4.56)))),
      LabeledPoint(0.0, Vectors.dense(1.01, 2.02, 3.03))
    ), 2)
    val tempDir = Utils.createTempDir()
    val outputDir = new File(tempDir, "output")
    //将LIBSVM格式的数据保存到指定文件
    MLUtils.saveAsLibSVMFile(examples, outputDir.toURI.toString)
    val lines = outputDir.listFiles()
      .filter(_.getName.startsWith("part-"))
      .flatMap(Source.fromFile(_).getLines())
      .toSet
    val expected = Set("1.1 1:1.23 3:4.56", "0.0 1:1.01 2:2.02 3:3.03")
    assert(lines === expected)
    Utils.deleteRecursively(tempDir)
  }

  test("appendBias") {//追加偏置
    //对向量增加偏置项,用于回归和分类算法计算中
    val sv = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))
    val sv1 = appendBias(sv).asInstanceOf[SparseVector]
    assert(sv1.size === 4)
    assert(sv1.indices === Array(0, 2, 3))
    assert(sv1.values === Array(1.0, 3.0, 1.0))

    val dv = Vectors.dense(1.0, 0.0, 3.0)
    val dv1 = appendBias(dv).asInstanceOf[DenseVector]
    assert(dv1.size === 4)
    assert(dv1.values === Array(1.0, 0.0, 3.0, 1.0))
  }

  test("kFold") {//K-折(倍)
    val data = sc.parallelize(1 to 100, 2)
    val collectedData = data.collect().sorted
    val twoFoldedRdd = kFold(data, 2, 1)
    assert(twoFoldedRdd(0)._1.collect().sorted === twoFoldedRdd(1)._2.collect().sorted)
    assert(twoFoldedRdd(0)._2.collect().sorted === twoFoldedRdd(1)._1.collect().sorted)
    for (folds <- 2 to 10) {
      for (seed <- 1 to 5) {
        val foldedRdds = kFold(data, folds, seed)
        assert(foldedRdds.size === folds)
        foldedRdds.map { case (training, validation) =>
          val result = validation.union(training).collect().sorted
          val validationSize = validation.collect().size.toFloat
          assert(validationSize > 0, "empty validation data")
          val p = 1 / folds.toFloat
          // Within 3 standard deviations of the mean
          //在平均值的3个标准偏差内
          val range = 3 * math.sqrt(100 * p * (1 - p))
          val expected = 100 * p
          val lowerBound = expected - range
          val upperBound = expected + range
          assert(validationSize > lowerBound,
            s"Validation data ($validationSize) smaller than expected ($lowerBound)" )
          assert(validationSize < upperBound,
            s"Validation data ($validationSize) larger than expected ($upperBound)" )
          assert(training.collect().size > 0, "empty training data")
          assert(result ===  collectedData,
            "Each training+validation set combined should contain all of the data.")
        }
        // K fold cross validation should only have each element in the validation set exactly once
        //K倍交叉验证应该只在验证中的每一个元素都有一次
        assert(foldedRdds.map(_._2).reduce((x, y) => x.union(y)).collect().sorted ===
          data.collect().sorted)
      }
    }
  }

  test("loadVectors") {//加载向量
    val vectors = sc.parallelize(Seq(
      Vectors.dense(1.0, 2.0),
      Vectors.sparse(2, Array(1), Array(-1.0)),
      Vectors.dense(0.0, 1.0)
    ), 2)
    val tempDir = Utils.createTempDir()
    val outputDir = new File(tempDir, "vectors")
    val path = outputDir.toURI.toString
    vectors.saveAsTextFile(path)
    val loaded = loadVectors(sc, path)
    assert(vectors.collect().toSet === loaded.collect().toSet)
    Utils.deleteRecursively(tempDir)
  }

  test("loadLabeledPoints") {//加载标记点
    val points = sc.parallelize(Seq(
      LabeledPoint(1.0, Vectors.dense(1.0, 2.0)),
      LabeledPoint(0.0, Vectors.sparse(2, Array(1), Array(-1.0))),
      LabeledPoint(1.0, Vectors.dense(0.0, 1.0))
    ), 2)
    val tempDir = Utils.createTempDir()
    val outputDir = new File(tempDir, "points")
    val path = outputDir.toURI.toString
    points.saveAsTextFile(path)
    val loaded = loadLabeledPoints(sc, path)
    assert(points.collect().toSet === loaded.collect().toSet)
    Utils.deleteRecursively(tempDir)
  }

  test("log1pExp") {//
    assert(log1pExp(76.3) ~== math.log1p(math.exp(76.3)) relTol 1E-10)
    assert(log1pExp(87296763.234) ~== 87296763.234 relTol 1E-10)

    assert(log1pExp(-13.8) ~== math.log1p(math.exp(-13.8)) absTol 1E-10)
    assert(log1pExp(-238423789.865) ~== math.log1p(math.exp(-238423789.865)) absTol 1E-10)
  }
}
