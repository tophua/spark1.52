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

package org.apache.spark.mllib.clustering

import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.{ DenseVector, SparseVector, Vector, Vectors }
import org.apache.spark.mllib.util.{ LocalClusterSparkContext, MLlibTestSparkContext }
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.util.Utils
/**
 * KMeans聚类
 */
class KMeansSuite extends SparkFunSuite with MLlibTestSparkContext {

  import org.apache.spark.mllib.clustering.KMeans.{ K_MEANS_PARALLEL, RANDOM }
  /**简单聚类**/
  test("single cluster") {
    val data = sc.parallelize(Array(
      Vectors.dense(1.0, 2.0, 6.0),
      Vectors.dense(1.0, 3.0, 0.0),
      Vectors.dense(1.0, 4.0, 6.0)))
    //聚类中心是坐标平均数
    val center = Vectors.dense(1.0, 3.0, 4.0)

    // No matter how many runs or iterations we use, we should get one cluster,
    //无论我们使用多少次运行或迭代，我们都应该得到一个集群
    // centered at the mean of the points 
    //以点的平均值为中心

    var model = KMeans.train(data, k = 1, maxIterations = 1)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = KMeans.train(data, k = 1, maxIterations = 2)
    //clusterCenters 聚类中心 
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = KMeans.train(data, k = 1, maxIterations = 5)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = KMeans.train(data, k = 1, maxIterations = 1, runs = 5)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = KMeans.train(data, k = 1, maxIterations = 1, runs = 5)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = KMeans.train(data, k = 1, maxIterations = 1, runs = 1, initializationMode = RANDOM)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = KMeans.train(
      data, k = 1, maxIterations = 1, runs = 1, initializationMode = K_MEANS_PARALLEL)
    assert(model.clusterCenters.head ~== center absTol 1E-5)
  }

  test("no distinct points") {//没有明显的点
    val data = sc.parallelize(
      Array(
        Vectors.dense(1.0, 2.0, 3.0),
        Vectors.dense(1.0, 2.0, 3.0),
        Vectors.dense(1.0, 2.0, 3.0)),
      2)
      //聚类中心是坐标平均数,
      val center = Vectors.dense(1.0, 2.0, 3.0)

    // Make sure code runs.
      //确保代码运行
    var model = KMeans.train(data, k = 2, maxIterations = 1)
    assert(model.clusterCenters.size === 2)
  }

  test("more clusters than points") {//更多的集群比点
    val data = sc.parallelize(
      Array(
        Vectors.dense(1.0, 2.0, 3.0),
        Vectors.dense(1.0, 3.0, 4.0)),
      2)

    // Make sure code runs.
      //确保代码运行
    var model = KMeans.train(data, k = 3, maxIterations = 1)
    assert(model.clusterCenters.size === 3)
  }
  //确定性的初始化
  test("deterministic initialization") {
    // Create a large-ish set of points for clustering
    //创建一个大的杂交点集的聚类
    val points = List.tabulate(1000)(n => Vectors.dense(n, n))
    val rdd = sc.parallelize(points, 3)

    for (initMode <- Seq(RANDOM, K_MEANS_PARALLEL)) {
      // Create three deterministic models and compare cluster means
      //创建三个确定性模型,并比较集群的方法
      val model1 = KMeans.train(rdd, k = 10, maxIterations = 2, runs = 1,
        initializationMode = initMode, seed = 42)
      val centers1 = model1.clusterCenters

      val model2 = KMeans.train(rdd, k = 10, maxIterations = 2, runs = 1,
        initializationMode = initMode, seed = 42)
      val centers2 = model2.clusterCenters

      centers1.zip(centers2).foreach {
        case (c1, c2) =>
          assert(c1 ~== c2 absTol 1E-14)
      }
    }
  }
//大数据集的单集群
  test("single cluster with big dataset") {
    val smallData = Array(
      Vectors.dense(1.0, 2.0, 6.0),
      Vectors.dense(1.0, 3.0, 0.0),
      Vectors.dense(1.0, 4.0, 6.0))
    val data = sc.parallelize((1 to 100).flatMap(_ => smallData), 4)

    // No matter how many runs or iterations we use, we should get one cluster,
    //无论我们使用多少次运行或迭代,我们应该得到一个集群
    // centered at the mean of the points
    //以点的平均值为中心

    val center = Vectors.dense(1.0, 3.0, 4.0)

    var model = KMeans.train(data, k = 1, maxIterations = 1)
    assert(model.clusterCenters.size === 1)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = KMeans.train(data, k = 1, maxIterations = 2)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = KMeans.train(data, k = 1, maxIterations = 5)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = KMeans.train(data, k = 1, maxIterations = 1, runs = 5)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = KMeans.train(data, k = 1, maxIterations = 1, runs = 5)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = KMeans.train(data, k = 1, maxIterations = 1, runs = 1, initializationMode = RANDOM)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = KMeans.train(data, k = 1, maxIterations = 1, runs = 1,
      initializationMode = K_MEANS_PARALLEL)
    assert(model.clusterCenters.head ~== center absTol 1E-5)
  }

  test("single cluster with sparse data") {//稀疏数据单簇

    val n = 10000
    val data = sc.parallelize((1 to 100).flatMap { i =>
      val x = i / 1000.0
      Array(
        Vectors.sparse(n, Seq((0, 1.0 + x), (1, 2.0), (2, 6.0))),
        Vectors.sparse(n, Seq((0, 1.0 - x), (1, 2.0), (2, 6.0))),
        Vectors.sparse(n, Seq((0, 1.0), (1, 3.0 + x))),
        Vectors.sparse(n, Seq((0, 1.0), (1, 3.0 - x))),
        Vectors.sparse(n, Seq((0, 1.0), (1, 4.0), (2, 6.0 + x))),
        Vectors.sparse(n, Seq((0, 1.0), (1, 4.0), (2, 6.0 - x))))
    }, 4)

    data.persist()

    // No matter how many runs or iterations we use, we should get one cluster,
    //无论我们使用多少次运行或迭代,我们应该得到一个集群
    // centered at the mean of the points
    //以点的平均值为中心

    val center = Vectors.sparse(n, Seq((0, 1.0), (1, 3.0), (2, 4.0)))

    var model = KMeans.train(data, k = 1, maxIterations = 1)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = KMeans.train(data, k = 1, maxIterations = 2)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = KMeans.train(data, k = 1, maxIterations = 5)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = KMeans.train(data, k = 1, maxIterations = 1, runs = 5)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = KMeans.train(data, k = 1, maxIterations = 1, runs = 5)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = KMeans.train(data, k = 1, maxIterations = 1, runs = 1, initializationMode = RANDOM)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = KMeans.train(data, k = 1, maxIterations = 1, runs = 1,
      initializationMode = K_MEANS_PARALLEL)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    data.unpersist()
  }

  test("k-means|| initialization") {//k-均值| |初始化

    case class VectorWithCompare(x: Vector) extends Ordered[VectorWithCompare] {
      override def compare(that: VectorWithCompare): Int = {
        if (this.x.toArray.foldLeft[Double](0.0)((acc, x) => acc + x * x) >
          that.x.toArray.foldLeft[Double](0.0)((acc, x) => acc + x * x)) {
          -1
        } else {
          1
        }
      }
    }

    val points = Seq(
      Vectors.dense(1.0, 2.0, 6.0),
      Vectors.dense(1.0, 3.0, 0.0),
      Vectors.dense(1.0, 4.0, 6.0),
      Vectors.dense(1.0, 0.0, 1.0),
      Vectors.dense(1.0, 1.0, 1.0))
    val rdd = sc.parallelize(points)

    // K-means|| initialization should place all clusters into distinct centers because
    // it will make at least five passes, and it will give non-zero probability to each
    //k-均值| |初始化应该把所有的集群在不同的中心，因为它会使至少五遍
    // unselected point as long as it hasn't yet selected all of them
    //它将为非零的概率对每个选中的点,只要它还没有选定的所有的人
    var model = KMeans.train(rdd, k = 5, maxIterations = 1)

    assert(model.clusterCenters.sortBy(VectorWithCompare(_))
      .zip(points.sortBy(VectorWithCompare(_))).forall(x => x._1 ~== (x._2) absTol 1E-5))

    // Iterations of Lloyd's should not change the answer either
      //劳埃德的迭代不应该改变答案
    model = KMeans.train(rdd, k = 5, maxIterations = 10)
    assert(model.clusterCenters.sortBy(VectorWithCompare(_))
      .zip(points.sortBy(VectorWithCompare(_))).forall(x => x._1 ~== (x._2) absTol 1E-5))

    // Neither should more runs
     //也不应该更多的运行
    model = KMeans.train(rdd, k = 5, maxIterations = 10, runs = 5)
    assert(model.clusterCenters.sortBy(VectorWithCompare(_))
      .zip(points.sortBy(VectorWithCompare(_))).forall(x => x._1 ~== (x._2) absTol 1E-5))
  }

  test("two clusters") {//两个聚类
    val points = Seq(
      Vectors.dense(0.0, 0.0),
      Vectors.dense(0.0, 0.1),
      Vectors.dense(0.1, 0.0),
      Vectors.dense(9.0, 0.0),
      Vectors.dense(9.0, 0.2),
      Vectors.dense(9.2, 0.0))
    val rdd = sc.parallelize(points, 3)

    for (initMode <- Seq(RANDOM, K_MEANS_PARALLEL)) {
      // Two iterations are sufficient no matter where the initial centers are.
      //两次迭代是足够的,无论在哪里初始中心
      val model = KMeans.train(rdd, k = 2, maxIterations = 2, runs = 1, initMode)

      val predicts = model.predict(rdd).collect()
      for(ps<-predicts){
        println(ps)
      }
      //预测分类
      assert(predicts(0) === predicts(1))
      assert(predicts(0) === predicts(2))
      assert(predicts(3) === predicts(4))
      assert(predicts(3) === predicts(5))
      assert(predicts(0) != predicts(3))
    }
  }

  test("model save/load") {//模型保存/负载
    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString

    Array(true, false).foreach {
      case selector =>
        val model = KMeansSuite.createModel(10, 3, selector)
        // Save model, load it back, and compare.
        //保存模型,加载它回来,并比较。
        try {
          model.save(sc, path)
          val sameModel = KMeansModel.load(sc, path)
          KMeansSuite.checkEqual(model, sameModel)
        } finally {
          Utils.deleteRecursively(tempDir)
        }
    }
  }

  test("Initialize using given cluster centers") {//使用给定的聚类中心初始化
    val points = Seq(
      Vectors.dense(0.0, 0.0),
      Vectors.dense(1.0, 0.0),
      Vectors.dense(0.0, 1.0),
      Vectors.dense(1.0, 1.0))
    val rdd = sc.parallelize(points, 3)
    // creating an initial model
    //创建初始模型
    val initialModel = new KMeansModel(Array(points(0), points(2)))

    val returnModel = new KMeans()
      .setK(2)
      .setMaxIterations(0)
      .setInitialModel(initialModel)
      .run(rdd)
    // comparing the returned model and the initial model
    //返回模型和初始模型的比较
    assert(returnModel.clusterCenters(0) === initialModel.clusterCenters(0))
    assert(returnModel.clusterCenters(1) === initialModel.clusterCenters(1))
  }

}

object KMeansSuite extends SparkFunSuite {
  def createModel(dim: Int, k: Int, isSparse: Boolean): KMeansModel = {
    val singlePoint = isSparse match {
      case true =>
        Vectors.sparse(dim, Array.empty[Int], Array.empty[Double])
      case _ =>
        Vectors.dense(Array.fill[Double](dim)(0.0))
    }
    new KMeansModel(Array.fill[Vector](k)(singlePoint))
  }

  def checkEqual(a: KMeansModel, b: KMeansModel): Unit = {
    assert(a.k === b.k)
    a.clusterCenters.zip(b.clusterCenters).foreach {
      case (ca: SparseVector, cb: SparseVector) =>
        assert(ca === cb)
      case (ca: DenseVector, cb: DenseVector) =>
        assert(ca === cb)
      case _ =>//checkequal失败以来,两簇是不相同的
        throw new AssertionError("checkEqual failed since the two clusters were not identical.\n")
    }
  }
}

class KMeansClusterSuite extends SparkFunSuite with LocalClusterSparkContext {
  //在训练和预测中，任务的大小应该是小的
  test("task size should be small in both training and prediction") {
    val m = 4
    val n = 200000
    val points = sc.parallelize(0 until m, 2).mapPartitionsWithIndex { (idx, iter) =>
      val random = new Random(idx)
      iter.map(i => Vectors.dense(Array.fill(n)(random.nextDouble)))
    }.cache()
    for (initMode <- Seq(KMeans.RANDOM, KMeans.K_MEANS_PARALLEL)) {
      // If we serialize data directly in the task closure, the size of the serialized task would be
      //如果我们将数据直接在任务结束,该系列任务的规模将大于1MB，因此Spark会抛出一个错误
      // greater than 1MB and hence Spark would throw an error.
      val model = KMeans.train(points, 2, 2, 1, initMode)
      val predictions = model.predict(points).collect()
      val cost = model.computeCost(points)
    }
  }
}
