package org.apache.spark.examples.ml
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{ IndexToString, StringIndexer, VectorAssembler }
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.SQLContext
import org.apache.spark.{ SparkConf, SparkContext }
/**
 * https://www.ibm.com/developerworks/cn/opensource/os-cn-spark-practice5/
 * 使用 ML Pipeline 构建机器学习工作流
 */
object ClassificationPipeline_IBM {
  def main(args: Array[String]) {
    /*if (args.length < 1) {
      println("Usage:ClassificationPipeline inputDataFile")
      sys.exit(1)
    }*/
    val conf = new SparkConf().setMaster("local[2]").setAppName("Classification with ML Pipeline")
    val sc = new SparkContext(conf)
    val sqlCtx = new SQLContext(sc)

    /**
     * 这是一个从纸币鉴别过程中的图片里提取的数据集，总共包含五个列，前 4 列是指标值 (连续型)，最后一列是真假标识
     * 四列依次是小波变换图像的方差，小波变换图像的偏态，小波变换图像的峰度，图像熵，类别标签
     * Step 1
     * Read the source data file and convert it to be a dataframe with columns named.
     * 3.6216,8.6661,-2.8073,-0.44699,0
     * 4.5459,8.1674,-2.4586,-1.4621,0
     * 3.866,-2.6383,1.9242,0.10645,0
     * 3.4566,9.5228,-4.0112,-3.5944,0
     * 0.32924,-4.4552,4.5718,-0.9888,0
     * ... ...
     */
    
    
     /***=======spark-shell.cmd执行命令===========
     val parsedRDD = sc.textFile("./ml-100k/data_banknote_authentication.txt").map(_.split(",")).map(eachRow => {
      val a = eachRow.map(x => x.toDouble)
      (a(0), a(1), a(2), a(3), a(4))
    })
 		 val df = sqlCtx.createDataFrame(parsedRDD).toDF(
      "f0", "f1", "f2", "f3", "label").cache()

      df.registerTempTable("data")

      sqlCtx.sql("select f0,f1,f2,f3,label from data ").show()
      **/
    
    val parsedRDD = sc.textFile("../data/mllib/data_banknote_authentication.txt").map(_.split(",")).map(eachRow => {
      val a = eachRow.map(x => x.toDouble)
      (a(0), a(1), a(2), a(3), a(4))
    })
    val df = sqlCtx.createDataFrame(parsedRDD).toDF(
      //四列依次是小波变换图像的方差，小波变换图像的偏态，小波变换图像的峰度，图像熵，类别标签,最后一列是真假标识
      "f0", "f1", "f2", "f3", "label").cache()
     df.registerTempTable("data")
   val queryCaseWhen = sqlCtx.sql("select f0,f1,f2,f3,label from data ").show()
   
   
  
    /**
     * *
     * Step 2
     * 使用 StringIndexer 去把源数据里的字符 Label，按照 Label 出现的频次对其进行序列编码, 如，0,1,2，…。
     * 在本例的数据中，可能这个步骤的作用不甚明显，因为我们的数据格式良好，Label 本身也只有两种，
     * 并且已经是类序列编码的”0”和”1”格式。但是对于多分类问题或者是 Label 本身是字符串的编码方式，
     * 如”High”,”Low”,”Medium”等，那么这个步骤就很有用，转换后的格式，才能被 Spark 更好的处理
     */
    val labelIndexer = new StringIndexer()
      .setInputCol("label") //
      .setOutputCol("indexedLabel")
      .fit(df)// fit 方法设计和实现上实际上是采用了模板方法的设计模式，具体会调用实现类的 train 方法

    /**
     * Step 3
     * 使用 VectorAssembler 从源数据中提取特征指标数据,这是一个比较典型且通用的步骤，
     * 因为我们的原始数据集里，经常会包含一些非指标数据，如 ID，Description 等
     * VectorAssembler是一个转换器,它将给定的若干列合并为一列向量
     */
    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("f0", "f1", "f2", "f3"))
      .setOutputCol("featureVector")

    /**
     * Step 4
     * 创建一个随机森林分类器 RandomForestClassifier 实例，并设定相关参数，
     * 主要是告诉随机森林算法输入 DataFrame 数据里哪个列是特征向量，哪个是类别标识.
     */
    val rfClassifier = new RandomForestClassifier()
      .setLabelCol("indexedLabel")//标签列的名称
      .setFeaturesCol("featureVector")//训练数据集 DataFrame 中存储特征数据的列名
      .setProbabilityCol("probability")//类别预测结果的条件概率值存储列的名称, 默认值是”probability”
      .setPredictionCol("prediction")//算法预测结果的存储列的名称, 默认是”prediction”
      .setNumTrees(5)//并告诉随机森林分类器训练 5 棵独立的子树

    /**
     * Step 5
     *我们使用 IndexToString Transformer 去把之前的序列编码后的 Label 转化成原始的 Label，恢复之前的可读性比较高的 Label，
     *这样不论是存储还是显示模型的测试结果，可读性都会比较高
     */
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    //Step 6
    //Randomly split the input data by 8:2, while 80% is for training, the rest is for testing.
     //随机将输入数据按8:2，而80%是用于训练，其余的用于测试
    val Array(trainingData, testData) = df.randomSplit(Array(0.8, 0.2))

    /**
     * Step 7
     * 构建 Pipeline 实例，并且会按照顺序执行，最终我们根据得到的 PipelineModel 实例，
     * 进一步调用其 transform 方法，去用训练好的模型预测测试数据集的分类,
     * 要构建一个 Pipeline，首先我们需要定义 Pipeline 中的各个 PipelineStage，如指标提取和转换模型训练等
     * 例如:al pipeline = new Pipeline().setStages(Array(stage1,stage2,stage3,…))
     * 然后就可以把训练数据集作为入参并调用 Pipelin 实例的 fit 方法来开始以流的方式来处理源训练数据，
     * 这个调用会返回一个 PipelineModel 类实例， 进而被用来预测测试数据的标签，它是一个 Transformer
     */
    val pipeline = new Pipeline().setStages(Array(labelIndexer, vectorAssembler, rfClassifier, labelConverter))
    val model = pipeline.fit(trainingData)

    /**
     * Step 8
     * Perform predictions about testing data. This transform method will return a result DataFrame
     * with new prediction column appended towards previous DataFrame.
     * 主要是用来把 一个 DataFrame 转换成另一个 DataFrame,比如一个模型就是一个 Transformer，
     * 因为它可以把 一个不包含预测标签的测试数据集 DataFrame 打上标签转化成另一个包含预测标签的 DataFrame，
     * 显然这样的结果集可以被用来做分析结果的可视化
     */
    val predictionResultDF = model.transform(testData) //主要是用来把 一个 DataFrame 转换成另一个 DataFrame

    /**
     * Step 9
     * Select features,label,and predicted label from the DataFrame to display.
     * 特征选择,标签,和预测的帧显示标签
     * We only show 20 rows, it is just for reference.
     * 我们只显示20行,它只是供参考
      +--------+--------+--------+---------+-----+--------------+
      |      f0|      f1|      f2|       f3|label|predictedLabel|
      +--------+--------+--------+---------+-----+--------------+
      |  4.3684|  9.6718| -3.9606|  -3.1625|  0.0|           0.0|
      |  1.1432| -3.7413|  5.5777| -0.63578|  0.0|           0.0|
      |-0.38214|  8.3909|  2.1624|  -3.7405|  0.0|           0.0|
      |-0.96511|  9.4111|  1.7305|  -4.8629|  0.0|           0.0|
      |  4.3239| -4.8835|  3.4356|  -0.5776|  0.0|           0.0|
      |  4.8265| 0.80287|  1.6371|   1.1875|  0.0|           0.0|
      |  2.5635|  6.7769|-0.61979|  0.38576|  0.0|           0.0|
      |   5.807|  5.0097| -2.2384|  0.43878|  0.0|           0.0|
      |  3.1377| -4.1096|  4.5701|  0.98963|  0.0|           0.0|
      |  4.2586| 11.2962| -4.0943|  -4.3457|  0.0|           0.0|
      |  1.7939| -1.1174|  1.5454| -0.26079|  0.0|           0.0|
      |  2.5367|   2.599|  2.0938|  0.20085|  0.0|           0.0|
      |  4.7181| 10.0153| -3.9486|  -3.8582|  0.0|           0.0|
      |  4.1654| -3.4495|   3.643|   1.0879|  0.0|           0.0|
      |  4.4069| 10.9072| -4.5775|  -4.4271|  0.0|           0.0|
      |  1.8664|  7.7763|-0.23849|  -2.9634|  0.0|           0.0|
      |  2.1616| -6.8804|  8.1517|-0.081048|  0.0|           0.0|
      |  5.1129|-0.49871| 0.62863|   1.1189|  0.0|           0.0|
      |  3.5438|  1.2395|   1.997|   2.1547|  0.0|           0.0|
      |  3.2351|   9.647| -3.2074|  -2.5948|  0.0|           0.0|
      +--------+--------+--------+---------+-----+--------------+
     */
    predictionResultDF.select("f0", "f1", "f2", "f3", "featureVector","label", "predictedLabel").show(20)

    /**
     * Step 10
     * The evaluator code is used to compute the prediction accuracy, this is
     * 计算代码是用来计算预测精度,
     * usually a valuable feature to estimate prediction accuracy the trained model.
     * 这通常是一个有价值的特征来估计预测的准确性训练模型
     */
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")//标签列的名称
      .setPredictionCol("prediction")//算法预测结果的存储列的名称, 默认是”prediction”
      .setMetricName("precision")//测量名称
    //predictionAccuracy: Double = 0.9825783972125436
    val predictionAccuracy = evaluator.evaluate(predictionResultDF)   
    //Testing Error = 0.017421602787456414
    println("Testing Error = " + (1.0 - predictionAccuracy))
    /**
     * Step 11(Optional)
     * You can choose to print or save the the model structure.
     * 您可以选择打印或保存模型结构
     */
    val randomForestModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
    println("Trained Random Forest Model is:\n" + randomForestModel.toDebugString)
  }
}