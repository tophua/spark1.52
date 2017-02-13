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

// scalastyle:off println
package org.apache.spark.examples.mllib

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.Vectors

/**
 * 降维主成分分析(PCA)
 * Compute the principal components of a tall-and-skinny matrix, whose rows are observations.
 * 计算一个主成分析又高又瘦的矩阵,观察行值
 * The input matrix must be stored in row-oriented dense format, one line per row with its entries
 * 输入矩阵必须以行为导向的密集格式存储,每行一行的条目由空格隔开
 * separated by space. For example,
 * {{{
 * 0.5 1.0
 * 2.0 3.0
 * 4.0 5.0
 * }}}
 * represents a 3-by-2 matrix, whose first row is (0.5, 1.0).
 * 代表3行2列矩阵,第一行是(0.5,1)
 * PCA算法步骤:
 * 设有m条n维数据
 *   1)将原始数据按列组成n行m列矩阵X
 *   2)将X的每一行(代表一个属性字段)进行零均值化,即减去这一行的均值
 *   3)求出协方差矩阵C=\frac{1}{m}XX^\mathsf{T}
 *   4)求出协方差矩阵的特征值及对应的特征向量
 *   5)将特征向量按对应特征值大小从上到下按行排列成矩阵,取前k行组成矩阵P
 *   6)Y=PX即为降维到k维后的数据
 */
object TallSkinnyPCA {
  def main(args: Array[String]) {
   /* if (args.length != 1) {
      System.err.println("Usage: TallSkinnyPCA <input>")
      System.exit(1)
    }*/

    val conf = new SparkConf().setAppName("TallSkinnyPCA").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Load and parse the data file.
    //加载和解析数据文件
    /**
     *{{{
     * 0.5 1.0
     * 2.0 3.0
     * 4.0 5.0
     * }}}
     */
    val rows = sc.textFile("../data/mllib/CosineSimilarity.txt").map { line =>
      val values = line.split(' ').map(_.toDouble)
      Vectors.dense(values)
    }
    //行矩阵(RowMatrix)按行分布式存储,无行索引,底层支撑结构是多行数据组成的RDD,每行是一个局部向量
    val mat = new RowMatrix(rows)
    ///numCols:2
    println("numCols:"+mat.numCols())
    // Compute principal components.
    //计算主成分
    val pc = mat.computePrincipalComponents(mat.numCols().toInt)
    /**  
      Principal components are:
      -0.6596045032796274  -0.7516128652792183  
      -0.7516128652792183  0.6596045032796274 
     */
    println("Principal components are:\n" + pc)

    sc.stop()
  }
}
// scalastyle:on println
