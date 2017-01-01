package org.apache.spark.examples.mllib
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.SparkConf
/**
 * Spark cookbook p127
 */
object SVMWithSGDExample {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("SVMWithSGDExample").setMaster("local[4]")
    val sc = new SparkContext(conf)
    //把数据加载成RDD
    val svmData = MLUtils.loadLibSVMFile(sc, "../data/mllib/sample_libsvm_data.txt")
    //计算记录的数目
    svmData.count
    //把数据集分成两半,一半训练数据和一半测试数据
    val trainingAndTest = svmData.randomSplit(Array(0.5, 0.5))
    //训练数据和测试数据赋值
    val trainingData = trainingAndTest(0)
    val testData = trainingAndTest(1)
    //训练算法产并经过100次迭代构建模型
    val model = SVMWithSGD.train(trainingData, 100)
    //用模型去为任意数据集预测标签,使用测试数据中的第一个点测试标签
    val label = model.predict(testData.first.features)
    //创建一个元组,其中第一个元素是测试数据的预测标签,第二个元素是实际标签
    val predictionsAndLabels = testData.map(r => (model.predict(r.features), r.label))
    //计算有多少预测标签和实际标签不匹配的记录
    predictionsAndLabels.filter(p => p._1 != p._2).count
  }
}