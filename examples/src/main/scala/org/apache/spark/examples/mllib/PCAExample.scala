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

import java.text.BreakIterator

import scala.collection.mutable

import scopt.OptionParser

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.rdd.RDD

/**
 * 主成分析的降维
 * 降维:降低维度或者降低特征数量的过程.
 * 为什么要做特征缩放:没有必要把特征放到同一个平等的级别(特征值减去平均值,然后除以标准差)
 * 房屋面积	占地面积 		缩放的房屋面积 		缩放的占地面积 		房屋价格
 * 2524,		12839,		-0.025,					-0.231,					2405
 * 2937,		10000,			0.323,				-0.4,						2200
 * 1778,		8040,			-0.654,					-0.517,					1400
 * 1242,		13104,		-1.105,					-0.215,					1800
 * 2900,		10000,			0.291,				-0.4,						2351
 * 1218,		3049,			 -1.126,				-0.814,					795
 * 2722,		38768,			0.142,				 1.312,					2725
 */
object PCAExample {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("KMeansClustering")
    val sc = new SparkContext(sparkConf)
    //加载数据到RDD
    val data = sc.textFile("../data/mllib/scaledhousedata.csv")
    //数据转换成密集向量的RDD
    val parsedData = data.map(line => Vectors.dense(line.
      split(',').map(_.toDouble)))
    //根据parsedData创建一个RowMatrix(行矩阵)
    val mat = new RowMatrix(parsedData)
    //计算一个主要(成)分析
    val pc = mat.computePrincipalComponents(1)
    /**
     * -0.9995710763570875
		 * -0.02928588926998105
     */
    //通过扩张主要成分把行投影到线性空间
    val projected = mat.multiply(pc)
    //把投影过的RowMatrix转换成RDD
    val projectedRDD = projected.rows
    
    projectedRDD.saveAsTextFile("phdata")

  }
}
// scalastyle:on println
