package org.apache.spark.examples.mllib

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{ IndexToString, StringIndexer, Word2Vec }
import org.apache.spark.sql.SQLContext
import org.apache.spark.{ SparkContext, SparkConf }
/**
 * Spark ML 的文本分类
 * 参考资料
 * https://www.ibm.com/developerworks/cn/opensource/os-cn-spark-practice6/
 */
object SMSClassifierWord2Vec_IBM {
  final val VECTOR_SIZE = 100
  def main(args: Array[String]) {
    /*   if (args.length < 1) {
      println("Usage:SMSClassifier SMSTextFile")
      sys.exit(1)
    }*/
    //LogUtils.setDefaultLogLevel()
    val conf = new SparkConf().setMaster("local[2]").setAppName("SMS Message Classification (HAM or SPAM)")
    val sc = new SparkContext(conf)
    val sqlCtx = new SQLContext(sc)
    //读取原始数据集，并创建一个 DataFrame
    val parsedRDD = sc.textFile("../data/mllib/SMSSpamCollection").map(_.split("\t")).map(eachRow => {
      (eachRow(0), eachRow(1).split(" "))
    })
    val msgDF = sqlCtx.createDataFrame(parsedRDD).toDF("label", "message")
    //使用 StringIndexer 将原始的文本标签 (“Ham”或者“Spam”) 转化成数值型的表型，以便 Spark ML 处理
    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(msgDF)
    //使用 Word2Vec 将短信文本转化成数值型词向量
    val word2Vec = new Word2Vec().setInputCol("message").setOutputCol("features").setVectorSize(VECTOR_SIZE).setMinCount(1)
    val layers = Array[Int](VECTOR_SIZE, 6, 5, 2)
    //使用 MultilayerPerceptronClassifier 训练一个多层感知器模型
    val mlpc = new MultilayerPerceptronClassifier().setLayers(layers).setBlockSize(512)
    .setSeed(1234L).setMaxIter(128).setFeaturesCol("features")
    .setLabelCol("indexedLabel").setPredictionCol("prediction")
    //使用 LabelConverter 将预测结果的数值标签转化成原始的文本标签
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
    //将原始文本数据按照 8:2 的比例分成训练和测试数据集
    val Array(trainingData, testData) = msgDF.randomSplit(Array(0.8, 0.2))

    val pipeline = new Pipeline().setStages(Array(labelIndexer, word2Vec, mlpc, labelConverter))
    val model = pipeline.fit(trainingData)

    val predictionResultDF = model.transform(testData)
    //below 2 lines are for debug use
    predictionResultDF.printSchema
    predictionResultDF.select("message", "label", "predictedLabel").show(30)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("precision")
    //最后在测试数据集上测试模型的预测精确度
    val predictionAccuracy = evaluator.evaluate(predictionResultDF)
    println("Testing Accuracy is %2.4f".format(predictionAccuracy * 100) + "%")
    sc.stop
  }
}