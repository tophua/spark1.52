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
package org.apache.spark.examples.ml

import java.io.File

import scopt.OptionParser

import org.apache.spark.examples.mllib.AbstractParams
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.util.Utils
import org.apache.spark.mllib.util.MLUtils
/**
 * An example of how to use [[org.apache.spark.sql.DataFrame]] for ML. Run with
 * {{{
 * ./bin/run-example ml.DataFrameExample [options]
 * }}}
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
object DataFrameExample {
/**
 *  libSVM的数据格式
 *  <label> <index1>:<value1> <index2>:<value2> ...
 *  其中<label>是训练数据集的目标值,对于分类,它是标识某类的整数(支持多个类);对于回归,是任意实数
 *  <index>是以1开始的整数,可以是不连续
 *  <value>为实数,也就是我们常说的自变量
 */
  case class Params(input: String = "../data/mllib/sample_libsvm_data.txt")
    extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("DataFrameExample") {
      head("DataFrameExample: an example app using DataFrame for ML.")
      opt[String]("input")
        .text(s"input path to dataframe")
        .action((x, c) => c.copy(input = x))
      checkConfig { params =>
        success
      }
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Params) {
   val conf = new SparkConf().setAppName("CountVectorizerExample").setMaster("local[4]")
    val sc = new SparkContext(conf)
  
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
 

    // Load input data
    //Loading LIBSVM file with UDT from ../data/mllib/sample_libsvm_data.txt.
    println(s"Loading LIBSVM file with UDT from ${params.input}.")
     val dataSVM=MLUtils.loadLibSVMFile(sc, params.input)
     
    val df: DataFrame = sqlContext.createDataFrame(dataSVM).cache()
    println("Schema from LIBSVM:")
    df.printSchema()
    //Loaded training data as a DataFrame with 100 records.
    println(s"Loaded training data as a DataFrame with ${df.count()} records.")

    // Show statistical summary of labels.
    //显示标签统计汇总
    val labelSummary = df.describe("label")
    /**+-------+------------------+
      *|summary|             label|
      *+-------+------------------+
      *|  count|               100|
      *|   mean|              0.57|
      *| stddev|0.4950757517794625|
      *|    min|               0.0|
      *|    max|               1.0|
      *+-------+------------------+**/
    labelSummary.show()

    // Convert features column to an RDD of vectors.
    //转换特征列向量法
    val features = df.select("features").rdd.map { case Row(v: Vector) => v }
   // val featureSummary = features.aggregate(new MultivariateOnlineSummarizer())(
      //(summary, feat) => summary.add(Vectors.fromML(feat)),
   //   (sum1, sum2) => sum1.merge(sum2))
   // println(s"Selected features column with average values:\n ${featureSummary.mean.toString}")

    // Save the records in a parquet file.
     //保存parquet记录文件
    val tmpDir = Utils.createTempDir()
    //C:\Users\liushuhua\AppData\Local\Temp\spark-f97fb832-d864-4855-ac6c-d38564a04de5\dataframe
    val outputDir = new File(tmpDir, "dataframe").toString
    println(s"Saving to $outputDir as Parquet file.")
    df.write.parquet(outputDir)

    // Load the records back.
    //重新加载数据
    println(s"Loading Parquet file with UDT from $outputDir.")
    val newDF = sqlContext.read.parquet(outputDir)
    println(s"Schema from Parquet:")
    newDF.printSchema()

    sc.stop()
  }
}
// scalastyle:on println
