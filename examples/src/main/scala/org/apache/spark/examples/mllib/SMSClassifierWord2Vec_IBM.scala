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
    //读取原始数据集,并创建一个 DataFrame
    //该数据集结构非常简单,只有两列,第一列是短信的标签 ,第二列是短信内容,两列之间用制表符 (tab) 分隔
    val parsedRDD = sc.textFile("../data/mllib/SMSSpamCollection").map(_.split("\t")).map(eachRow => {
      (eachRow(0), eachRow(1).split(" "))
    })
    val msgDF = sqlCtx.createDataFrame(parsedRDD).toDF("label", "message")
    //使用 StringIndexer 将原始的文本标签 (“Ham”或者“Spam”) 转化成数值型的表型,以便 Spark ML 处理
    //fit()方法将DataFrame转化为一个Transformer的算法
    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(msgDF)
    //使用 Word2Vec 将短信文本转化成数值型词向量
    val word2Vec = new Word2Vec().setInputCol("message").setOutputCol("features").setVectorSize(VECTOR_SIZE).setMinCount(1)
   //这个参数是一个整型数组类型,第一个元素需要和特征向量的维度相等,最后一个元素需要训练数据的标签取值个数相等,
    //如 2 分类问题就写 2。中间的元素有多少个就代表神经网络有多少个隐层,元素的取值代表了该层的神经元的个数。
    //例如val layers = Array[Int](100,6,5,2)
    val layers = Array[Int](VECTOR_SIZE, 6,  2)
    //使用 MultilayerPerceptronClassifier 训练一个多层感知器模型
    val mlpc = new MultilayerPerceptronClassifier().setLayers(layers).setBlockSize(512).setFeaturesCol("features")
    //
    .setMaxIter(128)
    //算法预测结果的存储列的名称, 默认是”prediction”
    .setLabelCol("indexedLabel").setPredictionCol("prediction")
    .setSeed(1234L)
    //使用 LabelConverter 将预测结果的数值标签转化成原始的文本标签
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
    //将原始文本数据按照 8:2 的比例分成训练和测试数据集
    val Array(trainingData, testData) = msgDF.randomSplit(Array(0.8, 0.2))
    //ML Pipeline 提供了大量做特征数据提取和转换的工具
     //PipeLine:将多个DataFrame和Estimator算法串成一个特定的ML Wolkflow
     //一个 Pipeline在结构上会包含一个或多个 PipelineStage,每一个 PipelineStage 都会完成一个任务
    val pipeline = new Pipeline().setStages(Array(labelIndexer, word2Vec, mlpc, labelConverter))
    val model = pipeline.fit(trainingData)//训练数据
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val predictionResultDF = model.transform(testData)
    //below 2 lines are for debug use
    //下面2行是调试使用
    predictionResultDF.printSchema //打印RDD中数据的表模式
    predictionResultDF.select("message", "label", "predictedLabel").show(30)

    val evaluator = new MulticlassClassificationEvaluator()
	//标签列的名称
      .setLabelCol("indexedLabel")
      //算法预测结果的存储列的名称, 默认是”prediction”
      .setPredictionCol("prediction")
      .setMetricName("precision")//度量方式 precision准确率
    //最后在测试数据集上测试模型的预测精确度
    val predictionAccuracy = evaluator.evaluate(predictionResultDF)
    println("Testing Accuracy is %2.4f".format(predictionAccuracy * 100) + "%")
    sc.stop
  }
}
