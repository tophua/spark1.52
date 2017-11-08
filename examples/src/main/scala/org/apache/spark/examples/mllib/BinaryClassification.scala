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

import org.apache.log4j.{Level, Logger}
import scopt.OptionParser

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.optimization.{SquaredL2Updater, L1Updater}

/**
 * 二分类的一个示例应用程序
 * An example app for binary classification. Run with
 * {{{
 * bin/run-example org.apache.spark.examples.mllib.BinaryClassification
 * }}}
 * 
 * A synthetic dataset is located at `data/mllib/sample_binary_classification_data.txt`.
 * 加载一个虚拟的数据集data/mllib/sample_binary_classification_data.txt
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 * 如果你使用模板创建一个App,请你使用Spark-submit 提交App
 */
object BinaryClassification {
  //算法类型
  object Algorithm extends Enumeration {
    type Algorithm = Value
    val SVM, LR = Value
  }
  //回归类型
  object RegType extends Enumeration {
    type RegType = Value
    val L1, L2 = Value
  }

  import Algorithm._
  import RegType._

  case class Params(
      input: String = "../data/mllib/sample_binary_classification_data.txt",
      numIterations: Int = 100,//迭代次数
      stepSize: Double = 1.0,//每次迭代优化步长
      algorithm: Algorithm = LR,//逻辑回归
      regType: RegType = L2,
      //正则化
      regParam: Double = 0.01) extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("BinaryClassification") {
      head("BinaryClassification: an example app for binary classification.")
      opt[Int]("numIterations")
        .text("number of iterations")
        .action((x, c) => c.copy(numIterations = x))
      opt[Double]("stepSize")//每次迭代优化步长
        .text("initial step size (ignored by logistic regression), " +
          s"default: ${defaultParams.stepSize}")
        .action((x, c) => c.copy(stepSize = x))
      opt[String]("algorithm")
        .text(s"algorithm (${Algorithm.values.mkString(",")}), " +
        s"default: ${defaultParams.algorithm}")
        .action((x, c) => c.copy(algorithm = Algorithm.withName(x)))
      opt[String]("regType")
        .text(s"regularization type (${RegType.values.mkString(",")}), " +
        s"default: ${defaultParams.regType}")
        .action((x, c) => c.copy(regType = RegType.withName(x)))
      opt[Double]("regParam")
        .text(s"regularization parameter, default: ${defaultParams.regParam}")
    //  arg[String]("<input>")
    //    .required()
	/**
	 *  libSVM的数据格式
	 *  <label> <index1>:<value1> <index2>:<value2> ...
	 *  其中<label>是训练数据集的目标值,对于分类,它是标识某类的整数(支持多个类);对于回归,是任意实数
	 *  <index>是以1开始的整数,可以是不连续
	 *  <value>为实数,也就是我们常说的自变量
	 */
    //    .text("input paths to labeled examples in LIBSVM format")
    //    .action((x, c) => c.copy(input = x))
      note(
        """
          |For example, the following command runs this app on a synthetic dataset:
          |
          | bin/spark-submit --class org.apache.spark.examples.mllib.BinaryClassification \
          |  examples/target/scala-*/spark-examples-*.jar \
          |  --algorithm LR --regType L2 --regParam 1.0 \
          |  data/mllib/sample_binary_classification_data.txt
        """.stripMargin)
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Params) {
    val conf = new SparkConf().setAppName(s"BinaryClassification with $params").setMaster("local[*]")
    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)
/**
 *  libSVM的数据格式
 *  <label> <index1>:<value1> <index2>:<value2> ...
 *  其中<label>是训练数据集的目标值,对于分类,它是标识某类的整数(支持多个类);对于回归,是任意实数
 *  <index>是以1开始的整数,可以是不连续
 *  <value>为实数,也就是我们常说的自变量
 */
    val examples = MLUtils.loadLibSVMFile(sc, params.input).cache()

    val splits = examples.randomSplit(Array(0.8, 0.2))
    val training = splits(0).cache()
    val test = splits(1).cache()

    val numTraining = training.count()
    val numTest = test.count()
    //Training: 73, test: 27.
    println(s"Training: $numTraining, test: $numTest.")

    examples.unpersist(blocking = false)

    val updater = params.regType match {
      case L1 => new L1Updater()
      case L2 => new SquaredL2Updater()
    }

    val model = params.algorithm match {
      case LR =>
        //基于lbfgs优化损失函数,支持多分类,(BFGS是逆秩2拟牛顿法)
        val algorithm = new LogisticRegressionWithLBFGS()
        algorithm.optimizer
          .setNumIterations(params.numIterations)//迭代数
          .setUpdater(updater)//
          .setRegParam(params.regParam)//正则化
        algorithm.run(training).clearThreshold()
      case SVM => //(SGD随机梯度下降)
        val algorithm = new SVMWithSGD()
        algorithm.optimizer
          .setNumIterations(params.numIterations)//迭代次数
          .setStepSize(params.stepSize)//每次迭代优化步长
          .setUpdater(updater)
          .setRegParam(params.regParam)//正则化
        algorithm.run(training).clearThreshold()
    }

    val prediction = model.predict(test.map(_.features))
    val predictionAndLabel = prediction.zip(test.map(_.label))

    val metrics = new BinaryClassificationMetrics(predictionAndLabel)
     //areaUnderPR平均准确率,通常评价结果的质量,平均准确率等于训练样本中被正确分类的数目除以样本总数   
    //Test areaUnderPR = 1.0.
    println(s"Test areaUnderPR = ${metrics.areaUnderPR()}.")
    //ROC曲线下面积,是一种用来度量分类模型好坏的一个标准
    //Test areaUnderROC = 1.0.
    println(s"Test areaUnderROC = ${metrics.areaUnderROC()}.")

    sc.stop()
  }
}
// scalastyle:on println
