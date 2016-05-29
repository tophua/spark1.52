package org.apache.spark.examples.mllib
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
/**
 * 线性支持向量机用于大规模分类任务的标准方法
 */
object SVMWithSGDDemo {
  def main(args: Array[String]) {
    // 屏蔽不必要的日志显示终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    // 设置运行环境
    val conf = new SparkConf().setAppName("SVMWithSGDDemo").setMaster("local[4]")
    val sc = new SparkContext(conf)
    // 加载LIBSVM格式数据    
    // Load and parse the data file
    // Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(sc, "../data/mllib/sample_libsvm_data.txt")
    // Split data into training (60%) and test (40%).
    //将数据切分训练数据(60%)和测试数据(40%)
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)
    //运行训练数据模型构建模型
    // Run training algorithm to build the model    
    val numIterations = 100
    val model = SVMWithSGD.train(training, numIterations)
    //清除默认阈值
    // Clear the default threshold.
    model.clearThreshold()
    //在测试数据上计算原始分数
    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      println(">>" + score + "\t" + point.label)
      (score, point.label)
    }
    //获得评估指标
    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()
    //平均准确率
    println("Area under ROC = " + auROC)

    
    /**逻辑回归***/
    //运行训练算法构建模型
    val modelBFGS = new LogisticRegressionWithLBFGS()
      .setNumClasses(10)
      .run(training)
    //在测试数据上计算原始分数
    // Compute raw scores on the test set.
    val predictionAndLabels = test.map {
      case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
    }
    //获取评估指标
    // Get evaluation metrics.
    val metricsBFGS = new MulticlassMetrics(predictionAndLabels)
    val precision = metricsBFGS.precision
    println("Precision = " + precision)

  }

}