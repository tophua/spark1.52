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

package org.apache.spark.ml.clustering

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.clustering.{KMeans => MLlibKMeans}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

private[clustering] case class TestRow(features: Vector)
/**
 * 聚类套件
 */
object KMeansSuite {
  def generateKMeansData(sql: SQLContext, rows: Int, dim: Int, k: Int): DataFrame = {
    val sc = sql.sparkContext
    val rdd = sc.parallelize(1 to rows).map(i => Vectors.dense(Array.fill(dim)((i % k).toDouble)))
      .map(v => new TestRow(v))
    sql.createDataFrame(rdd)
  }
}

class KMeansSuite extends SparkFunSuite with MLlibTestSparkContext {

  final val k = 5
  @transient var dataset: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    dataset = KMeansSuite.generateKMeansData(sqlContext, 50, 3, k)
  }

  test("default parameters") {
    //KMeans默认参数
    val kmeans = new KMeans()
    //聚类簇数
    assert(kmeans.getK === 2)
    //特征列名
    assert(kmeans.getFeaturesCol === "features")
    //Prediction预测列名
    assert(kmeans.getPredictionCol === "prediction")
    //最大迭代次数
    assert(kmeans.getMaxIter === 20)
    //初始化模型k-means||
    assert(kmeans.getInitMode === MLlibKMeans.K_MEANS_PARALLEL)
    //初始化步骤
    assert(kmeans.getInitSteps === 5)
    //迭代算法的收敛性
    assert(kmeans.getTol === 1e-4)
  }

  test("set parameters") {//设置参数
    val kmeans = new KMeans()
      .setK(9)
      //设置特征列
      .setFeaturesCol("test_feature")
      //设置预测列
      .setPredictionCol("test_prediction")
      //最大迭代次数
      .setMaxIter(33)
      //设置模型
      .setInitMode(MLlibKMeans.RANDOM)
      //设置初始化步长
      .setInitSteps(3)
      //设置种子
      .setSeed(123)
      //迭代算法的收敛性
      .setTol(1e-3)

    assert(kmeans.getK === 9)
    assert(kmeans.getFeaturesCol === "test_feature")
    assert(kmeans.getPredictionCol === "test_prediction")
    assert(kmeans.getMaxIter === 33)
    assert(kmeans.getInitMode === MLlibKMeans.RANDOM)
    assert(kmeans.getInitSteps === 3)
    assert(kmeans.getSeed === 123)
    assert(kmeans.getTol === 1e-3)
  }

  test("parameters validation") {//参数验证
    intercept[IllegalArgumentException] {
      new KMeans().setK(1)
    }
    intercept[IllegalArgumentException] {
      new KMeans().setInitMode("no_such_a_mode")
    }
    intercept[IllegalArgumentException] {
      new KMeans().setInitSteps(0)
    }
  }

  test("fit & transform") {//适合&转换
    val predictionColName = "kmeans_prediction"
    //PredictionCol 测量输出的名称
    val kmeans = new KMeans().setK(k).setPredictionCol(predictionColName).setSeed(1)
    /**dataset:List([[1.0,1.0,1.0]], [[2.0,2.0,2.0]], [[3.0,3.0,3.0]], [[4.0,4.0,4.0]], 
    [[0.0,0.0,0.0]], [[1.0,1.0,1.0]], [[2.0,2.0,2.0]], [[3.0,3.0,3.0]], [[4.0,4.0,4.0]], 
    [[0.0,0.0,0.0]], [[1.0,1.0,1.0]], [[2.0,2.0,2.0]], [[3.0,3.0,3.0]], [[4.0,4.0,4.0]], 
    [[0.0,0.0,0.0]], [[1.0,1.0,1.0]], [[2.0,2.0,2.0]], [[3.0,3.0,3.0]], [[4.0,4.0,4.0]],
    [[0.0,0.0,0.0]], [[1.0,1.0,1.0]], [[2.0,2.0,2.0]], [[3.0,3.0,3.0]], [[4.0,4.0,4.0]], 
    [[0.0,0.0,0.0]], [[1.0,1.0,1.0]], [[2.0,2.0,2.0]], [[3.0,3.0,3.0]], [[4.0,4.0,4.0]], 
    [[0.0,0.0,0.0]], [[1.0,1.0,1.0]], [[2.0,2.0,2.0]], [[3.0,3.0,3.0]], [[4.0,4.0,4.0]], 
    [[0.0,0.0,0.0]], [[1.0,1.0,1.0]], [[2.0,2.0,2.0]], [[3.0,3.0,3.0]], [[4.0,4.0,4.0]], 
    [[0.0,0.0,0.0]], [[1.0,1.0,1.0]], [[2.0,2.0,2.0]], [[3.0,3.0,3.0]], [[4.0,4.0,4.0]], 
    [[0.0,0.0,0.0]], [[1.0,1.0,1.0]], [[2.0,2.0,2.0]], [[3.0,3.0,3.0]], [[4.0,4.0,4.0]], 
    [[0.0,0.0,0.0]])**/
    //fit()方法将DataFrame转化为一个Transformer的算法
    val model = kmeans.fit(dataset)//返回一个训练模型
    //clusterCenters = Array([1.0,1.0,1.0], [4.0,4.0,4.0], [0.0,0.0,0.0], [3.0,3.0,3.0], [2.0,2.0,2.0])
    assert(model.clusterCenters.length === k)
    //println("dataset:"+dataset.collect().toSeq)
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val transformed = model.transform(dataset)//转换成DataFrame
    //期望值列
    val expectedColumns = Array("features", predictionColName)
    expectedColumns.foreach { column =>
      //println("column>>>>"+column)
      //features,kmeans_prediction
      assert(transformed.columns.contains(column))
    }
    val coll=transformed.select("features","kmeans_prediction").collect()
   /**
    * res2: Array[org.apache.spark.sql.Row] = Array([[1.0,1.0,1.0],0], [[2.0,2.0,2.0],4],
    * [[3.0,3.0,3.0],3], [[4.0,4.0,4.0],1], [[0.0,0.0,0.0],2], [[1.0,1.0,1.0],0],
			[[2.0,2.0,2.0],4], [[3.0,3.0,3.0],3], [[4.0,4.0,4.0],1], [[0.0,0.0,0.0],2], 
			[[1.0,1.0,1.0],0], [[2.0,2.0,2.0],4], [[3.0,3.0,3.0],3], [[4.0,4.0,4.0],1], 
			[[0.0,0.0,0.0],2], [[1.0,1.0,1.0],0], [[2.0,2.0,2.0],4], [[3.0,3.0,3.0],3], 
			[[4.0,4.0,4.0],1], [[0.0,0.0,0.0],2], [[1.0,1.0,1.0],0], [[2.0,2.0,2.0],4], 
			[[3.0,3.0,3.0],3], [[4.0,4.0,4.0],1], [[0.0,0.0,0.0],2], [[1.0,1.0,1.0],0], 
			[[2.0,2.0,2.0],4], [[3.0,3.0,3.0],3], [[4.0,4.0,4.0],1], [[0.0,0.0,0.0],2], 
			[[1.0,1.0,1.0],0], [[2.0,2.0,2.0],4], [[3.0,3.0,3.0],3], [[4.0,4.0,4.0],1], 
			[[0.0,0.0,0.0],2], [[1.0,1.0,1.0],0], [[2.0,2.0,2.0],4], [[3.0,3.0,3.0],3], 
    */

    val clusters = transformed.select(predictionColName).map(_.getInt(0)).distinct().collect().toSet
    assert(clusters.size === k)
    assert(clusters === Set(0, 1, 2, 3, 4))
  }
}
