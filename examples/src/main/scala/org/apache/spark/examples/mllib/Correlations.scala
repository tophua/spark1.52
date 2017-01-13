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

import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 一个示例应用程序来自文件中的汇总多元数据
 * An example app for summarizing multivariate data from a file. Run with
 * {{{
 * bin/run-example org.apache.spark.examples.mllib.Correlations
 * }}}
 * By default, this loads a synthetic dataset from `data/mllib/sample_linear_regression_data.txt`.
 * 默认使用,加载虚拟数据集data/mllib/sample_linear_regression_data.txt
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app. 
 */
object Correlations {

  case class Params(input: String = "../data/mllib/sample_linear_regression_data.txt")
    extends AbstractParams[Params]

  def main(args: Array[String]) {

    val defaultParams = Params()

    val parser = new OptionParser[Params]("Correlations") {
      head("Correlations: an example app for computing correlations")
      opt[String]("input")
        .text(s"Input path to labeled examples in LIBSVM format, default: ${defaultParams.input}")
        .action((x, c) => c.copy(input = x))
      note(
        """
        |For example, the following command runs this app on a synthetic dataset:
        |例如,下面的命令在虚拟数据集上运行这个应用程序：
        | bin/spark-submit --class org.apache.spark.examples.mllib.Correlations \
        |  examples/target/scala-*/spark-examples-*.jar \
        |  --input data/mllib/sample_linear_regression_data.txt
        """.stripMargin)
    }

    parser.parse(args, defaultParams).map { params =>{
      //println("========="+params)
      run(params)
    }
    } getOrElse {
      
    
        sys.exit(1)
    }
  }

  def run(params: Params) {
    val conf = new SparkConf().setAppName(s"Correlations with $params").setMaster("local[*]")
    val sc = new SparkContext(conf)
/**
 *  libSVM的数据格式
 *  <label> <index1>:<value1> <index2>:<value2> ...
 *  其中<label>是训练数据集的目标值,对于分类,它是标识某类的整数(支持多个类);对于回归,是任意实数
 *  <index>是以1开始的整数,可以是不连续
 *  <value>为实数,也就是我们常说的自变量
 */
    val examples = MLUtils.loadLibSVMFile(sc, params.input).cache()
    //Summary of data file: ../data/mllib/sample_linear_regression_data.txt
    println(s"Summary of data file: ${params.input}")
    //501 data points
    println(s"${examples.count()} data points")

    // Calculate label -- feature correlations
    //计算标签-特征相关性
    val labelRDD = examples.map(_.label)
    val numFeatures = examples.take(1)(0).features.size
    val corrType = "pearson"
    println()
    //Correlation (pearson) between label and each feature
    //标签和每个特征之间的相关性(皮尔森)
    println(s"Correlation ($corrType) between label and each feature")
    println(s"Feature\tCorrelation")
    var feature = 0
    while (feature < numFeatures) {
      val featureRDD = examples.map(_.features(feature))
      val corr = Statistics.corr(labelRDD, featureRDD)
      //9	   0.03452069517112544
      println(s"$feature\t$corr")
      feature += 1
    }
    println()

    sc.stop()
  }
}
// scalastyle:on println
