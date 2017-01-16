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

package org.apache.spark.mllib.feature

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.{ DenseVector, SparseVector, Vector, Vectors }
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.mllib.stat.{ MultivariateStatisticalSummary, MultivariateOnlineSummarizer }
import org.apache.spark.rdd.RDD
/**
 * StandardScaler标准化:将数据按期属性(按列进行)减去其均值,并处以其方差。
 *       得到的结果是,对于每个属性/每列来说所有数据都聚集在0附近,方差为1
 * 数据标准化(Z-score)标准化方法:给予原始数据的均值(mean)和标准差（standard deviation）进行数据的标准化。
 * 经过处理的数据符合标准正态分布,即均值为0,标准差为1
 * StandardScaler将某个特征向量(由所有样本某一个特征组成的向量)进行标准化,使数据均值为0,方差为1
 */
class StandardScalerSuite extends SparkFunSuite with MLlibTestSparkContext {

  // When the input data is all constant, the variance is zero. The standardization against
  // zero variance is not well-defined, but we decide to just set it into zero here.
  //密集向量(dense vector)使用double数组表示元素值
  val constantData = Array(
    Vectors.dense(2.0),
    Vectors.dense(2.0),
    Vectors.dense(2.0))
  //稀疏向量(sparse vector)通过两个并列的数组来表示：一个表示索引，一个表示数值
  val sparseData = Array(
    //3表示此向量的长度，后面的比较直观，Seq里面每一对都是(索引，值）的形式。
    Vectors.sparse(3, Seq((0, -2.0), (1, 2.3))),
    Vectors.sparse(3, Seq((1, -1.0), (2, -3.0))),
    Vectors.sparse(3, Seq((1, -5.1))),
    Vectors.sparse(3, Seq((0, 3.8), (2, 1.9))),
    Vectors.sparse(3, Seq((0, 1.7), (1, -0.6))),
    Vectors.sparse(3, Seq((1, 1.9))))

  val denseData = Array(//密集的数据
    Vectors.dense(-2.0, 2.3, 0),
    Vectors.dense(0.0, -1.0, -3.0),
    Vectors.dense(0.0, -5.1, 0.0),
    Vectors.dense(3.8, 0.0, 1.9),
    Vectors.dense(1.7, -0.6, 0.0),
    Vectors.dense(0.0, 1.9, 0.0))

  private def computeSummary(data: RDD[Vector]): MultivariateStatisticalSummary = {
    data.treeAggregate(new MultivariateOnlineSummarizer)(
      (aggregator, data) => aggregator.add(data),
      (aggregator1, aggregator2) => aggregator1.merge(aggregator2))
  }
  //标准化与密集的输入时,提供了标准
  test("Standardization with dense input when means and stds are provided") {

    val dataRDD = sc.parallelize(denseData, 3)
    //标准化是指：对于训练集中的样本，基于列统计信息将数据除以方差或（且）者将数据减去其均值（结果是方差等于1，数据在0附近）
    //标准化可以提升模型优化阶段的收敛速度，还可以避免方差很大的特征对模型训练产生过大的影响
    /**
     * withMean 默认值False. True表示均值正则化(每个值减去均值)
     *          这会导致密集型输出,所以在稀疏数据上无效
     * withStd 默认值True. 每个值除以标准差进行缩
     */
    val standardizer1 = new StandardScaler(withMean = true, withStd = true)
    val standardizer2 = new StandardScaler()
    //withStd=true,将方差缩放到1,withMean-将均值移到0,注意对于稀疏输入矩阵不可以用。默认为false
    val standardizer3 = new StandardScaler(withMean = true, withStd = false)
    //fit 计算汇总统计信息,然后返回一个模型,该模型可以根据StandardScaler配置将输入数据转换为标准差为1,均值为0的特征
    val model1 = standardizer1.fit(dataRDD)
    //fit()方法将DataFrame转化为一个Transformer的算法
    val model2 = standardizer2.fit(dataRDD)
    val model3 = standardizer3.fit(dataRDD)

    val equivalentModel1 = new StandardScalerModel(model1.std, model1.mean)
    //第一个True表示均值正则化(每个值减去均值),第二个True表示正则化标准差(每个值除以标准差进行缩放)
    val equivalentModel2 = new StandardScalerModel(model2.std, model2.mean, true, false)
    val equivalentModel3 = new StandardScalerModel(model3.std, model3.mean, false, true)

    val data1 = denseData.map(equivalentModel1.transform)
    val data2 = denseData.map(equivalentModel2.transform)
    val data3 = denseData.map(equivalentModel3.transform)
 //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val data1RDD = equivalentModel1.transform(dataRDD)
    val data2RDD = equivalentModel2.transform(dataRDD)
    val data3RDD = equivalentModel3.transform(dataRDD)

    val summary = computeSummary(dataRDD)
    val summary1 = computeSummary(data1RDD)
    val summary2 = computeSummary(data2RDD)
    val summary3 = computeSummary(data3RDD)

    assert((denseData, data1, data1RDD.collect()).zipped.forall {
      case (v1: DenseVector, v2: DenseVector, v3: DenseVector) => true
      case (v1: SparseVector, v2: SparseVector, v3: SparseVector) => true
      case _ => false //标准化后应保留向量类型
    }, "The vector type should be preserved after standardization.")

    assert((denseData, data2, data2RDD.collect()).zipped.forall {
      case (v1: DenseVector, v2: DenseVector, v3: DenseVector) => true
      case (v1: SparseVector, v2: SparseVector, v3: SparseVector) => true
      case _ => false
    }, "The vector type should be preserved after standardization.")

    assert((denseData, data3, data3RDD.collect()).zipped.forall {
      case (v1: DenseVector, v2: DenseVector, v3: DenseVector) => true
      case (v1: SparseVector, v2: SparseVector, v3: SparseVector) => true
      case _ => false
    }, "The vector type should be preserved after standardization.")

    assert((data1, data1RDD.collect()).zipped.forall((v1, v2) => v1 ~== v2 absTol 1E-5))
    assert((data2, data2RDD.collect()).zipped.forall((v1, v2) => v1 ~== v2 absTol 1E-5))
    assert((data3, data3RDD.collect()).zipped.forall((v1, v2) => v1 ~== v2 absTol 1E-5))
    //平均值
    assert(summary1.mean ~== Vectors.dense(0.0, 0.0, 0.0) absTol 1E-5)
    //方差
    assert(summary1.variance ~== Vectors.dense(1.0, 1.0, 1.0) absTol 1E-5)

    assert(summary2.mean !~== Vectors.dense(0.0, 0.0, 0.0) absTol 1E-5)
    assert(summary2.variance ~== Vectors.dense(1.0, 1.0, 1.0) absTol 1E-5)

    assert(summary3.mean ~== Vectors.dense(0.0, 0.0, 0.0) absTol 1E-5)
    assert(summary3.variance ~== summary.variance absTol 1E-5)

    assert(data1(0) ~== Vectors.dense(-1.31527964, 1.023470449, 0.11637768424) absTol 1E-5)
    assert(data1(3) ~== Vectors.dense(1.637735298, 0.156973995, 1.32247368462) absTol 1E-5)
    assert(data2(4) ~== Vectors.dense(0.865538862, -0.22604255, 0.0) absTol 1E-5)
    assert(data2(5) ~== Vectors.dense(0.0, 0.71580142, 0.0) absTol 1E-5)
    assert(data3(1) ~== Vectors.dense(-0.58333333, -0.58333333, -2.8166666666) absTol 1E-5)
    assert(data3(5) ~== Vectors.dense(-0.58333333, 2.316666666, 0.18333333333) absTol 1E-5)
  }
  
  test("Standardization with dense input") {//标准化的密集输入

    val dataRDD = sc.parallelize(denseData, 3)
    //第一个True表示均值正则化(每个值减去均值),第二个True表示正则化标准差(每个值除以标准差进行缩放)
    val standardizer1 = new StandardScaler(withMean = true, withStd = true)
    val standardizer2 = new StandardScaler()
    //第一个True表示均值正则化(每个值减去均值),第二个True表示正则化标准差(每个值除以标准差进行缩放)
    val standardizer3 = new StandardScaler(withMean = true, withStd = false)
    //fit()方法将DataFrame转化为一个Transformer的算法
    val model1 = standardizer1.fit(dataRDD)
    val model2 = standardizer2.fit(dataRDD)
    val model3 = standardizer3.fit(dataRDD)

    val data1 = denseData.map(model1.transform)
    val data2 = denseData.map(model2.transform)
    val data3 = denseData.map(model3.transform)
    //使用该类的好处在于可以保存训练集中的参数(均值、方差)直接使用其对象转换测试集数据
    val data1RDD = model1.transform(dataRDD)
    val data2RDD = model2.transform(dataRDD)
    val data3RDD = model3.transform(dataRDD)

    val summary = computeSummary(dataRDD)
    val summary1 = computeSummary(data1RDD)
    val summary2 = computeSummary(data2RDD)
    val summary3 = computeSummary(data3RDD)

    assert((denseData, data1, data1RDD.collect()).zipped.forall {
      case (v1: DenseVector, v2: DenseVector, v3: DenseVector) => true
      case (v1: SparseVector, v2: SparseVector, v3: SparseVector) => true
      case _ => false
      //标准化后应保留矢量类型
    }, "The vector type should be preserved after standardization.")

    assert((denseData, data2, data2RDD.collect()).zipped.forall {
      case (v1: DenseVector, v2: DenseVector, v3: DenseVector) => true
      case (v1: SparseVector, v2: SparseVector, v3: SparseVector) => true
      case _ => false
      //标准化后应保留矢量类型
    }, "The vector type should be preserved after standardization.")

    assert((denseData, data3, data3RDD.collect()).zipped.forall {
      case (v1: DenseVector, v2: DenseVector, v3: DenseVector) => true
      case (v1: SparseVector, v2: SparseVector, v3: SparseVector) => true
      case _ => false
      //标准化后应保留矢量类型
    }, "The vector type should be preserved after standardization.")

    assert((data1, data1RDD.collect()).zipped.forall((v1, v2) => v1 ~== v2 absTol 1E-5))
    assert((data2, data2RDD.collect()).zipped.forall((v1, v2) => v1 ~== v2 absTol 1E-5))
    assert((data3, data3RDD.collect()).zipped.forall((v1, v2) => v1 ~== v2 absTol 1E-5))

    assert(summary1.mean ~== Vectors.dense(0.0, 0.0, 0.0) absTol 1E-5)
    assert(summary1.variance ~== Vectors.dense(1.0, 1.0, 1.0) absTol 1E-5)

    assert(summary2.mean !~== Vectors.dense(0.0, 0.0, 0.0) absTol 1E-5)
    assert(summary2.variance ~== Vectors.dense(1.0, 1.0, 1.0) absTol 1E-5)

    assert(summary3.mean ~== Vectors.dense(0.0, 0.0, 0.0) absTol 1E-5)
    assert(summary3.variance ~== summary.variance absTol 1E-5)

    assert(data1(0) ~== Vectors.dense(-1.31527964, 1.023470449, 0.11637768424) absTol 1E-5)
    assert(data1(3) ~== Vectors.dense(1.637735298, 0.156973995, 1.32247368462) absTol 1E-5)
    assert(data2(4) ~== Vectors.dense(0.865538862, -0.22604255, 0.0) absTol 1E-5)
    assert(data2(5) ~== Vectors.dense(0.0, 0.71580142, 0.0) absTol 1E-5)
    assert(data3(1) ~== Vectors.dense(-0.58333333, -0.58333333, -2.8166666666) absTol 1E-5)
    assert(data3(5) ~== Vectors.dense(-0.58333333, 2.316666666, 0.18333333333) absTol 1E-5)
  }  
  //标准化稀疏输入
  test("Standardization with sparse input when means and stds are provided") {

    val dataRDD = sc.parallelize(sparseData, 3)
    //第一个True表示均值正则化(每个值减去均值),第二个True表示正则化标准差(每个值除以标准差进行缩放)
    val standardizer1 = new StandardScaler(withMean = true, withStd = true)
    val standardizer2 = new StandardScaler()
    val standardizer3 = new StandardScaler(withMean = true, withStd = false)
    //fit()方法将DataFrame转化为一个Transformer的算法
    val model1 = standardizer1.fit(dataRDD)
    val model2 = standardizer2.fit(dataRDD)
    val model3 = standardizer3.fit(dataRDD)

    val equivalentModel1 = new StandardScalerModel(model1.std, model1.mean)
    val equivalentModel2 = new StandardScalerModel(model2.std, model2.mean, true, false)
    val equivalentModel3 = new StandardScalerModel(model3.std, model3.mean, false, true)

    val data2 = sparseData.map(equivalentModel2.transform)
    //标准化不能应用于稀疏输入
    withClue("Standardization with mean can not be applied on sparse input.") {
      intercept[IllegalArgumentException] {
        sparseData.map(equivalentModel1.transform)
      }
    }
    //标准化不能应用于稀疏输入
    withClue("Standardization with mean can not be applied on sparse input.") {
      intercept[IllegalArgumentException] {
        sparseData.map(equivalentModel3.transform)
      }
    }
 //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val data2RDD = equivalentModel2.transform(dataRDD)

    val summary = computeSummary(data2RDD)

    assert((sparseData, data2, data2RDD.collect()).zipped.forall {
      case (v1: DenseVector, v2: DenseVector, v3: DenseVector) => true
      case (v1: SparseVector, v2: SparseVector, v3: SparseVector) => true
      case _ => false
    }, "The vector type should be preserved after standardization.")

    assert((data2, data2RDD.collect()).zipped.forall((v1, v2) => v1 ~== v2 absTol 1E-5))

    assert(summary.mean !~== Vectors.dense(0.0, 0.0, 0.0) absTol 1E-5)
    assert(summary.variance ~== Vectors.dense(1.0, 1.0, 1.0) absTol 1E-5)

    assert(data2(4) ~== Vectors.sparse(3, Seq((0, 0.865538862), (1, -0.22604255))) absTol 1E-5)
    assert(data2(5) ~== Vectors.sparse(3, Seq((1, 0.71580142))) absTol 1E-5)
  }
  //稀疏输入的标准化
  test("Standardization with sparse input") {

    val dataRDD = sc.parallelize(sparseData, 3)
    //第一个True表示均值正则化(每个值减去均值),第二个True表示正则化标准差(每个值除以标准差进行缩放)
    val standardizer1 = new StandardScaler(withMean = true, withStd = true)
    val standardizer2 = new StandardScaler()
    //第一个True表示均值正则化(每个值减去均值),第二个True表示正则化标准差(每个值除以标准差进行缩放)
    val standardizer3 = new StandardScaler(withMean = true, withStd = false)
    //fit()方法将DataFrame转化为一个Transformer的算法
    val model1 = standardizer1.fit(dataRDD)
    val model2 = standardizer2.fit(dataRDD)
    val model3 = standardizer3.fit(dataRDD)

    val data2 = sparseData.map(model2.transform)
    //标准化不能应用于稀疏输入
    withClue("Standardization with mean can not be applied on sparse input.") {
      intercept[IllegalArgumentException] {
        sparseData.map(model1.transform)
      }
    }
    //标准化不能应用于稀疏输入
    withClue("Standardization with mean can not be applied on sparse input.") {
      intercept[IllegalArgumentException] {
        sparseData.map(model3.transform)
      }
    }
 //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val data2RDD = model2.transform(dataRDD)

    val summary = computeSummary(data2RDD)

    assert((sparseData, data2, data2RDD.collect()).zipped.forall {
      case (v1: DenseVector, v2: DenseVector, v3: DenseVector) => true
      case (v1: SparseVector, v2: SparseVector, v3: SparseVector) => true
      case _ => false
      //标准化后应保留矢量类型
    }, "The vector type should be preserved after standardization.")

    assert((data2, data2RDD.collect()).zipped.forall((v1, v2) => v1 ~== v2 absTol 1E-5))

    assert(summary.mean !~== Vectors.dense(0.0, 0.0, 0.0) absTol 1E-5)
    assert(summary.variance ~== Vectors.dense(1.0, 1.0, 1.0) absTol 1E-5)
    assert(data2(4) ~== Vectors.sparse(3, Seq((0, 0.865538862), (1, -0.22604255))) absTol 1E-5)
    assert(data2(5) ~== Vectors.sparse(3, Seq((1, 0.71580142))) absTol 1E-5)
  }
  //标准化常数输入时提供手段和传播
  test("Standardization with constant input when means and stds are provided") {

    val dataRDD = sc.parallelize(constantData, 2)
   //第一个True表示均值正则化(每个值减去均值),第二个True表示正则化标准差(每个值除以标准差进行缩放)
    val standardizer1 = new StandardScaler(withMean = true, withStd = true)
    val standardizer2 = new StandardScaler(withMean = true, withStd = false)
    val standardizer3 = new StandardScaler(withMean = false, withStd = true)
     //fit()方法将DataFrame转化为一个Transformer的算法
    val model1 = standardizer1.fit(dataRDD)
    val model2 = standardizer2.fit(dataRDD)
    val model3 = standardizer3.fit(dataRDD)

    val equivalentModel1 = new StandardScalerModel(model1.std, model1.mean)
    //第一个True表示均值正则化(每个值减去均值),第二个True表示正则化标准差(每个值除以标准差进行缩放)
    val equivalentModel2 = new StandardScalerModel(model2.std, model2.mean, true, false)
    val equivalentModel3 = new StandardScalerModel(model3.std, model3.mean, false, true)

    val data1 = constantData.map(equivalentModel1.transform)
    val data2 = constantData.map(equivalentModel2.transform)
    val data3 = constantData.map(equivalentModel3.transform)

    assert(data1.forall(_.toArray.forall(_ == 0.0)),//方差为零,因此变换后的结果为0
      "The variance is zero, so the transformed result should be 0.0")
    assert(data2.forall(_.toArray.forall(_ == 0.0)),
      "The variance is zero, so the transformed result should be 0.0")
    assert(data3.forall(_.toArray.forall(_ == 0.0)),
      "The variance is zero, so the transformed result should be 0.0")
  }
  //常量输入标准化
  test("Standardization with constant input") {

    val dataRDD = sc.parallelize(constantData, 2)
    //第一个True表示均值正则化(每个值减去均值),第二个True表示正则化标准差(每个值除以标准差进行缩放)
    val standardizer1 = new StandardScaler(withMean = true, withStd = true)
    val standardizer2 = new StandardScaler(withMean = true, withStd = false)
    val standardizer3 = new StandardScaler(withMean = false, withStd = true)
    //fit()方法将DataFrame转化为一个Transformer的算法
    val model1 = standardizer1.fit(dataRDD)
    val model2 = standardizer2.fit(dataRDD)
    val model3 = standardizer3.fit(dataRDD)

    val data1 = constantData.map(model1.transform)
    val data2 = constantData.map(model2.transform)
    val data3 = constantData.map(model3.transform)

    assert(data1.forall(_.toArray.forall(_ == 0.0)),
      "The variance is zero, so the transformed result should be 0.0")
    assert(data2.forall(_.toArray.forall(_ == 0.0)),
      "The variance is zero, so the transformed result should be 0.0")
    assert(data3.forall(_.toArray.forall(_ == 0.0)),
      "The variance is zero, so the transformed result should be 0.0")
  }
  //标准模型的参数为空的妥善处理定标器
  test("StandardScalerModel argument nulls are properly handled") {

    withClue("model needs at least one of std or mean vectors") {//模型需要至少一个标准或平均向量
      intercept[IllegalArgumentException] {
        val model = new StandardScalerModel(null, null)
      }
    }
    withClue("model needs std to set withStd to true") {//模型需要两个标准
      intercept[IllegalArgumentException] {
        val model = new StandardScalerModel(null, Vectors.dense(0.0))
	//将数据缩放(尺度变换)到单位标准差
        model.setWithStd(true)
      }
    }
    withClue("model needs mean to set withMean to true") {//模型需要建立真正的平均值
      intercept[IllegalArgumentException] {
        val model = new StandardScalerModel(Vectors.dense(0.0), null)
	//在尺度变换(除方差)之前使用均值做居中处理(减去均值)
        model.setWithMean(true)
      }
    }
    //模型需要的标准和平均向量是相同的大小,当两个都提供
    withClue("model needs std and mean vectors to be equal size when both are provided") {
      intercept[IllegalArgumentException] {
        val model = new StandardScalerModel(Vectors.dense(0.0), Vectors.dense(0.0, 1.0))
      }
    }
  }
}
