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
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.MLTestingUtils
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Vector, Vectors, DenseMatrix, Matrices}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.mllib.feature.{PCAModel => OldPCAModel}
import org.apache.spark.sql.Row
/**
 * PCA主成分分析经常用于减少数据集的维数,同时保持数据集中的对方差贡献最大的特征。
 * 其方法主要是通过对协方差矩阵进行特征分解,以得出数据的主成分(即特征向量)与它们的权值(即特征值)
 * 
 */
class PCASuite extends SparkFunSuite with MLlibTestSparkContext {

  test("params") {//参数
    ParamsSuite.checkParams(new PCA)
    val mat = Matrices.dense(2, 2, Array(0.0, 1.0, 2.0, 3.0)).asInstanceOf[DenseMatrix]
    val model = new PCAModel("pca", new OldPCAModel(2, mat))
    ParamsSuite.checkParams(model)
  }

  test("pca") {//主成分分析
    val data = Array(
      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
    )

    val dataRDD = sc.parallelize(data, 2)
   //行矩阵(RowMatrix)按行分布式存储,无行索引,底层支撑结构是多行数据组成的RDD,每行是一个局部向量
    val mat = new RowMatrix(dataRDD)
     println("numRows:"+mat.numRows()+"\t numCols:"+mat.numCols())
    val pc = mat.computePrincipalComponents(3)//将维度降为3
    /**
     * pc:-0.44859172075072673  -0.28423808214073987  0.07290753685535857   
     *    0.13301985745398526   -0.05621155904253121  0.04139694704366137   
     *    -0.1252315635978212   0.7636264774662965    -0.5812910463866972   
     *    0.21650756651919933   -0.5652958773533948   -0.795459443083758    
     *    -0.8476512931126826   -0.11560340501314653  -0.14938466335164732 
     */
    println("pc numRows:"+pc.numRows+"\t numCols:"+pc.numCols)
    //multiply 矩阵相乘积操作
    val expected = mat.multiply(pc).rows//multiply 矩阵相乘积操作
    println("expected numRows:"+mat.multiply(pc).numRows()+"\t"+expected)
    //zip:Set(((5,[1,3],[1.0,7.0]),[1.6485728230883807,-4.013282700516295,-5.526819154542645]), 
    //([2.0,0.0,3.0,4.0,5.0],[-4.645104331781534,-1.1167972663619021,-5.526819154542643]), 
    //([4.0,0.0,0.0,6.0,7.0],[-6.428880535676489,-5.337951427775354,-5.526819154542644]))

    println("zip:"+dataRDD.zip(expected).collect().toSet)
    val df = sqlContext.createDataFrame(dataRDD.zip(expected)).toDF("features", "expected")

    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pca_features")
      .setK(3)
      .fit(df)//fit()方法将DataFrame转化为一个Transformer的算法

    // copied model must have the same parent.
    MLTestingUtils.checkCopy(pca)
/**
 * x:[1.6485728230883807,-4.013282700516295,-5.526819154542645]	 y:[1.6485728230883807,-4.013282700516295,-5.526819154542645]
 * x:[-4.645104331781534,-1.1167972663619021,-5.526819154542643] y:[-4.645104331781534,-1.1167972663619021,-5.526819154542643]
 * x:[-6.428880535676489,-5.337951427775354,-5.526819154542644]	 y:[-6.428880535676489,-5.337951427775354,-5.526819154542644]
 */
  //transform()方法将DataFrame转化为另外一个DataFrame的算法
    pca.transform(df).select("pca_features", "expected").collect().foreach {
    
      case Row(x: Vector, y: Vector) =>
        println("x:"+x+"\t y:"+y)
        //assert(x ~== y absTol 1e-5, "Transformed vector is different with expected vector.")
    }
  }
}
