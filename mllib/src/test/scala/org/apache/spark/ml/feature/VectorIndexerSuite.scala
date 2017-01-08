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

import scala.beans.{BeanInfo, BeanProperty}

import org.apache.spark.{Logging, SparkException, SparkFunSuite}
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.MLTestingUtils
import org.apache.spark.mllib.linalg.{SparseVector, Vector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
//向量索引 单词向量它用1和0分别表示是否存在某个词
class VectorIndexerSuite extends SparkFunSuite with MLlibTestSparkContext with Logging {

  import VectorIndexerSuite.FeatureData

  // identical, of length 3 相同的长度为3
  @transient var densePoints1: DataFrame = _
  @transient var sparsePoints1: DataFrame = _
  @transient var point1maxes: Array[Double] = _

  // identical, of length 2 相同的长度为2
  @transient var densePoints2: DataFrame = _
  @transient var sparsePoints2: DataFrame = _

  // different lengths 不同的长度
  @transient var badPoints: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val densePoints1Seq = Seq(//密集点1序列
      Vectors.dense(1.0, 2.0, 0.0),
      Vectors.dense(0.0, 1.0, 2.0),
      Vectors.dense(0.0, 0.0, -1.0),
      Vectors.dense(1.0, 3.0, 2.0))
    val sparsePoints1Seq = Seq(
      Vectors.sparse(3, Array(0, 1), Array(1.0, 2.0)),
      Vectors.sparse(3, Array(1, 2), Array(1.0, 2.0)),
      Vectors.sparse(3, Array(2), Array(-1.0)),
      Vectors.sparse(3, Array(0, 1, 2), Array(1.0, 3.0, 2.0)))
    point1maxes = Array(1.0, 3.0, 2.0)

    val densePoints2Seq = Seq(
      Vectors.dense(1.0, 1.0, 0.0, 1.0),
      Vectors.dense(0.0, 1.0, 1.0, 1.0),
      Vectors.dense(-1.0, 1.0, 2.0, 0.0))
    val sparsePoints2Seq = Seq(
      Vectors.sparse(4, Array(0, 1, 3), Array(1.0, 1.0, 1.0)),
      Vectors.sparse(4, Array(1, 2, 3), Array(1.0, 1.0, 1.0)),
      Vectors.sparse(4, Array(0, 1, 2), Array(-1.0, 1.0, 2.0)))

    val badPointsSeq = Seq(
      Vectors.sparse(2, Array(0, 1), Array(1.0, 1.0)),
      Vectors.sparse(3, Array(2), Array(-1.0)))

    // Sanity checks for assumptions made in tests
    //健全检查在测试中所作的假设
    assert(densePoints1Seq.head.size == sparsePoints1Seq.head.size)
    assert(densePoints2Seq.head.size == sparsePoints2Seq.head.size)
    assert(densePoints1Seq.head.size != densePoints2Seq.head.size)
    def checkPair(dvSeq: Seq[Vector], svSeq: Seq[Vector]): Unit = {
      assert(dvSeq.zip(svSeq).forall { case (dv, sv) => dv.toArray === sv.toArray },
        "typo in unit test")
    }
    checkPair(densePoints1Seq, sparsePoints1Seq)
    checkPair(densePoints2Seq, sparsePoints2Seq)

    densePoints1 = sqlContext.createDataFrame(sc.parallelize(densePoints1Seq, 2).map(FeatureData))
    sparsePoints1 = sqlContext.createDataFrame(sc.parallelize(sparsePoints1Seq, 2).map(FeatureData))
    densePoints2 = sqlContext.createDataFrame(sc.parallelize(densePoints2Seq, 2).map(FeatureData))
    sparsePoints2 = sqlContext.createDataFrame(sc.parallelize(sparsePoints2Seq, 2).map(FeatureData))
    badPoints = sqlContext.createDataFrame(sc.parallelize(badPointsSeq, 2).map(FeatureData))
  }

  private def getIndexer: VectorIndexer =
    new VectorIndexer().setInputCol("features").setOutputCol("indexed")

  test("params") {//参数
    ParamsSuite.checkParams(new VectorIndexer)
    val model = new VectorIndexerModel("indexer", 1, Map.empty)
    ParamsSuite.checkParams(model)
  }

  test("Cannot fit an empty DataFrame") {//不适合一个空数据集
    val rdd = sqlContext.createDataFrame(sc.parallelize(Array.empty[Vector], 2).map(FeatureData))
    val vectorIndexer = getIndexer
    intercept[IllegalArgumentException] {
    //fit()方法将DataFrame转化为一个Transformer的算法
      vectorIndexer.fit(rdd)
    }
  }
  //当给定不同RDDS大小的向量抛出异常
  test("Throws error when given RDDs with different size vectors") {
    val vectorIndexer = getIndexer
    //fit()方法将DataFrame转化为一个Transformer的算法
    val model = vectorIndexer.fit(densePoints1) // vectors of length 3 长度为3的向量
    
    // copied model must have the same parent.
    //复制的模型必须有相同的父
    MLTestingUtils.checkCopy(model)
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    model.transform(densePoints1) // should work
    model.transform(sparsePoints1) // should work
    intercept[SparkException] {
     //transform()方法将DataFrame转化为另外一个DataFrame的算法
      model.transform(densePoints2).collect()
      logInfo("Did not throw error when fit, transform were called on vectors of different lengths")
    }
    intercept[SparkException] {
      vectorIndexer.fit(badPoints)
      logInfo("Did not throw error when fitting vectors of different lengths in same RDD.")
    }
  }

  test("Same result with dense and sparse vectors") {//密集和稀疏向量的相同的结果
    def testDenseSparse(densePoints: DataFrame, sparsePoints: DataFrame): Unit = {
      val denseVectorIndexer = getIndexer.setMaxCategories(2)
      val sparseVectorIndexer = getIndexer.setMaxCategories(2)
      //fit()方法将DataFrame转化为一个Transformer的算法
      val denseModel = denseVectorIndexer.fit(densePoints)
      val sparseModel = sparseVectorIndexer.fit(sparsePoints)
      val denseMap = denseModel.categoryMaps
      val sparseMap = sparseModel.categoryMaps
      assert(denseMap.keys.toSet == sparseMap.keys.toSet,
        "Categorical features chosen from dense vs. sparse vectors did not match.")
      assert(denseMap == sparseMap,
        "Categorical feature value indexes chosen from dense vs. sparse vectors did not match.")
    }
    testDenseSparse(densePoints1, sparsePoints1)
    testDenseSparse(densePoints2, sparsePoints2)
  }
  //建立有效的分类特征值指数,正确转换,检查元数据
  test("Builds valid categorical feature value index, transform correctly, check metadata") {
    def checkCategoryMaps(
        data: DataFrame,
        maxCategories: Int,
        categoricalFeatures: Set[Int]): Unit = {
      val collectedData = data.collect().map(_.getAs[Vector](0))
      val errMsg = s"checkCategoryMaps failed for input with maxCategories=$maxCategories," +
        s" categoricalFeatures=${categoricalFeatures.mkString(", ")}"
      try {
        val vectorIndexer = getIndexer.setMaxCategories(maxCategories)
	//fit()方法将DataFrame转化为一个Transformer的算法
        val model = vectorIndexer.fit(data)
        val categoryMaps = model.categoryMaps
        // Chose correct categorical features
        //选择正确的分类特征
        assert(categoryMaps.keys.toSet === categoricalFeatures)
        val transformed = model.transform(data).select("indexed")
        val indexedRDD: RDD[Vector] = transformed.map(_.getAs[Vector](0))
        val featureAttrs = AttributeGroup.fromStructField(transformed.schema("indexed"))
        assert(featureAttrs.name === "indexed")
        assert(featureAttrs.attributes.get.length === model.numFeatures)
        categoricalFeatures.foreach { feature: Int =>
          val origValueSet = collectedData.map(_(feature)).toSet
          val targetValueIndexSet = Range(0, origValueSet.size).toSet
          val catMap = categoryMaps(feature)
          assert(catMap.keys.toSet === origValueSet) // Correct categories 正确的分类
          assert(catMap.values.toSet === targetValueIndexSet) // Correct category indices 正确的分类指标
          if (origValueSet.contains(0.0)) {
            assert(catMap(0.0) === 0) // value 0 gets index 0 值0获取索引0
          }
          // Check transformed data
          //检查转换数据
          assert(indexedRDD.map(_(feature)).collect().toSet === targetValueIndexSet)
          // Check metadata
          //检查元数据
          val featureAttr = featureAttrs(feature)
          assert(featureAttr.index.get === feature)
          featureAttr match {
            case attr: BinaryAttribute =>
              assert(attr.values.get === origValueSet.toArray.sorted.map(_.toString))
            case attr: NominalAttribute =>
              assert(attr.values.get === origValueSet.toArray.sorted.map(_.toString))
              assert(attr.isOrdinal.get === false)
            case _ =>
              throw new RuntimeException(errMsg + s". Categorical feature $feature failed" +
                s" metadata check. Found feature attribute: $featureAttr.")
          }
        }
        // Check numerical feature metadata.
        //检查数值特征元数据
        Range(0, model.numFeatures).filter(feature => !categoricalFeatures.contains(feature))
          .foreach { feature: Int =>
          val featureAttr = featureAttrs(feature)
          featureAttr match {
            case attr: NumericAttribute =>
              assert(featureAttr.index.get === feature)
            case _ =>
              throw new RuntimeException(errMsg + s". Numerical feature $feature failed" +
                s" metadata check. Found feature attribute: $featureAttr.")
          }
        }
      } catch {
        case e: org.scalatest.exceptions.TestFailedException =>
          logError(errMsg)
          throw e
      }
    }
    checkCategoryMaps(densePoints1, maxCategories = 2, categoricalFeatures = Set(0))
    checkCategoryMaps(densePoints1, maxCategories = 3, categoricalFeatures = Set(0, 2))
    checkCategoryMaps(densePoints2, maxCategories = 2, categoricalFeatures = Set(1, 3))
  }

  test("Maintain sparsity for sparse vectors") {//保持稀疏向量的稀疏性
    def checkSparsity(data: DataFrame, maxCategories: Int): Unit = {
      val points = data.collect().map(_.getAs[Vector](0))
      val vectorIndexer = getIndexer.setMaxCategories(maxCategories)
      //fit()方法将DataFrame转化为一个Transformer的算法
      val model = vectorIndexer.fit(data)
       //transform()方法将DataFrame转化为另外一个DataFrame的算法
      val indexedPoints = model.transform(data).select("indexed").map(_.getAs[Vector](0)).collect()
      points.zip(indexedPoints).foreach {
        case (orig: SparseVector, indexed: SparseVector) =>
          assert(orig.indices.length == indexed.indices.length)
        case _ => throw new UnknownError("Unit test has a bug in it.") // should never happen
      }
    }
    checkSparsity(sparsePoints1, maxCategories = 2)
    checkSparsity(sparsePoints2, maxCategories = 2)
  }

  test("Preserve metadata") {//保存元数据
    // For continuous features, preserve name and stats.
    //对于连续的功能,保存名称和统计
    val featureAttributes: Array[Attribute] = point1maxes.zipWithIndex.map { case (maxVal, i) =>
      NumericAttribute.defaultAttr.withName(i.toString).withMax(maxVal)
    }
    val attrGroup = new AttributeGroup("features", featureAttributes)
    val densePoints1WithMeta =
      densePoints1.select(densePoints1("features").as("features", attrGroup.toMetadata()))
    val vectorIndexer = getIndexer.setMaxCategories(2)
    //fit()方法将DataFrame转化为一个Transformer的算法
    val model = vectorIndexer.fit(densePoints1WithMeta)
    // Check that ML metadata are preserved.
    //检查是否保留了元数据的元数据。
    val indexedPoints = model.transform(densePoints1WithMeta)
    val transAttributes: Array[Attribute] =
      AttributeGroup.fromStructField(indexedPoints.schema("indexed")).attributes.get
    featureAttributes.zip(transAttributes).foreach { case (orig, trans) =>
      assert(orig.name === trans.name)
      (orig, trans) match {
        case (orig: NumericAttribute, trans: NumericAttribute) =>
          assert(orig.max.nonEmpty && orig.max === trans.max)
        case _ =>
          // do nothing
          // TODO: Once input features marked as categorical are handled correctly, check that here.
      }
    }
  }
}

private[feature] object VectorIndexerSuite {
  @BeanInfo
  case class FeatureData(@BeanProperty features: Vector)
}
