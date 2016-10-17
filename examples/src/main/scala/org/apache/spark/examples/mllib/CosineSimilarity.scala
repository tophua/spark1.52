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
 * Compute the similar columns of a matrix, using cosine similarity(相似度度量).
 * 计算一个矩阵的相似列，使用余弦相似性
 * The input matrix must be stored in row-oriented dense format, one line per row with its entries
 * 输入矩阵必须以行为导向的密集格式存储,每行一行，其条目由空格隔开
 * separated by space. For example,
 * {{{
 * 0.5 1.0
 * 2.0 3.0
 * 4.0 5.0
 * }}}
 * represents a 3-by-2 matrix, whose first row is (0.5, 1.0).
 * 代表3-by-2矩阵的第一行是(0.5,1)
 *
 * Example invocation:
 *
 * bin/run-example mllib.CosineSimilarity \
 * --threshold 0.1 data/mllib/sample_svm_data.txt
 */
object CosineSimilarity {
  case class Params(inputFile: String = null, threshold: Double = 0.1)
    extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("CosineSimilarity") {
      head("CosineSimilarity: an example app.")
      opt[Double]("threshold")
        .required()
        .text(s"threshold similarity: to tradeoff computation vs quality estimate")
        .action((x, c) => c.copy(threshold = x))
      arg[String]("<inputFile>")
        .required()
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
    val conf = new SparkConf().setAppName("CosineSimilarity")
    val sc = new SparkContext(conf)

    // Load and parse the data file.
    //加载和解析数据文件
    val rows = sc.textFile(params.inputFile).map { line =>
      val values = line.split(' ').map(_.toDouble)
      Vectors.dense(values)
    }.cache()
    val mat = new RowMatrix(rows)

    // Compute similar columns perfectly, with brute force.
    //完美地计算类似的列,使用强力
    val exact = mat.columnSimilarities()

    // Compute similar columns with estimation using DIMSUM
    //估计用点心计算类似的列
    val approx = mat.columnSimilarities(params.threshold)

    val exactEntries = exact.entries.map { case MatrixEntry(i, j, u) => ((i, j), u) }
    val approxEntries = approx.entries.map { case MatrixEntry(i, j, v) => ((i, j), v) }
    val MAE = exactEntries.leftOuterJoin(approxEntries).values.map {
      case (u, Some(v)) =>
        math.abs(u - v)
      case (u, None) =>
        math.abs(u)
    }.mean()

    println(s"Average absolute error in estimate is: $MAE")

    sc.stop()
  }
}
// scalastyle:on println
