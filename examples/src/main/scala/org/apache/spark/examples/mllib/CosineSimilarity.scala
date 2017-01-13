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

import scopt.OptionParser

import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, RowMatrix}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Compute the similar columns of a matrix, using cosine similarity(余弦相似度).
 * 计算一个矩阵的相似列,使用余弦相似性
 * The input matrix must be stored in row-oriented dense format, one line per row with its entries
 * 输入矩阵必须以行为导向的密集格式存储,每行一行,其条目由空格隔开
 * separated by space. For example,
 * {{{
 * 0.5 1.0
 * 2.0 3.0
 * 4.0 5.0
 * }}}
 * represents a 3-by-2 matrix, whose first row is (0.5, 1.0).
 * 代表3行2列矩阵的第一行是(0.5,1)
 *
 * Example invocation:
 *
 * bin/run-example mllib.CosineSimilarity \
 * --threshold 0.1 data/mllib/sample_svm_data.txt
 */
object CosineSimilarity {
 /* case class Params(inputFile: String = null, threshold: Double = 0.1)
    extends AbstractParams[Params]*/
  //阈值 threshold
    case class Params(inputFile: String = "../data/mllib/sample_svm_data.txt", threshold: Double = 0.1)
    extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("CosineSimilarity") {
      head("CosineSimilarity: an example app.")
      opt[Double]("threshold")
        //.required()
        .text(s"threshold similarity: to tradeoff computation vs quality estimate")
        .action((x, c) => c.copy(threshold = x))
        //arg[String]("<inputFile>")
       opt[String]("<inputFile>")
        //.required()
        .text(s"input file, one row per line, space-separated")
        .action((x, c) => c.copy(inputFile = x))
      note(
        """
          |For example, the following command runs this app on a dataset:
          |
          | ./bin/spark-submit  --class org.apache.spark.examples.mllib.CosineSimilarity \
          | examplesjar.jar \
          | --threshold 0.1 data/mllib/sample_svm_data.txt
        """.stripMargin)
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      System.exit(1)
    }
  }

  def run(params: Params) {
    val conf = new SparkConf().setAppName("CosineSimilarity").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Load and parse the data file.
    //加载和解析数据文件
    val rows = sc.textFile(params.inputFile).map { line =>
      val values = line.split(' ').map(x=>{
        println(x.toDouble)  
        x.toDouble
        })
       //创建一个稠密向量
      Vectors.dense(values)
    }.cache()
    val mat = new RowMatrix(rows)

    // Compute similar columns perfectly, with brute force.
    //计算矩阵中每两列之间的余弦相似度
    //参数:使用近似算法的阈值,值越大则运算速度越快而误差越大,默认值为0
    val exact = mat.columnSimilarities()

    // Compute similar columns with estimation using DIMSUM
    //计算矩阵中每两列之间的余弦相似度
    //参数:使用近似算法的阈值,值越大则运算速度越快而误差越大,默认为0
    val approx = mat.columnSimilarities(params.threshold)
    //MatrixEntry参数i行的索引,j列的索引,实体的值
    val exactEntries = exact.entries.map { case MatrixEntry(i, j, u) => 
      println(i+"=="+j+"==="+u)
      ((i, j), u) 
      }
    val approxEntries = approx.entries.map { case MatrixEntry(i, j, v) => ((i, j), v) }
     //MAE平均绝对误差是所有单个观测值与算术平均值的偏差的绝对值的平均
    val MAE = exactEntries.leftOuterJoin(approxEntries).values.map {
      case (u, Some(v)) =>
        math.abs(u - v)
      case (u, None) =>
        math.abs(u)
    }.mean()
    //MAE平均绝对误差是所有单个观测值与算术平均值的偏差的绝对值的平均
    //Average absolute error in estimate is: 0.052006398205651366
    println(s"Average absolute error in estimate is: $MAE")

    sc.stop()
  }
}
// scalastyle:on println
