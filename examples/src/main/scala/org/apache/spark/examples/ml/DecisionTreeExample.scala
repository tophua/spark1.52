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

import scala.collection.mutable
import scala.language.reflectiveCalls

import scopt.OptionParser

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.examples.mllib.AbstractParams
import org.apache.spark.ml.{Pipeline, PipelineStage, Transformer}
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.feature.{VectorIndexer, StringIndexer}
import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, DecisionTreeRegressor}
import org.apache.spark.ml.util.MetadataUtils
import org.apache.spark.mllib.evaluation.{RegressionMetrics, MulticlassMetrics}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SQLContext, DataFrame}


/**
 * An example runner for decision trees. Run with
 * 决策树的一个例子
 * {{{
 * ./bin/run-example ml.DecisionTreeExample [options]
 * }}}
 * Note that Decision Trees can take a large amount of memory.  If the run-example command above
 * 请注意,决策树可以采取大量的内存,如果上面的运行示例命令失败,试运行通过Spark提交指定的内存量为至少1G
 * fails, try running via spark-submit and specifying the amount of memory as at least 1g.
 * For local mode, run 本地模式
 * {{{
 * ./bin/spark-submit --class org.apache.spark.examples.ml.DecisionTreeExample --driver-memory 1g
 *   [examples JAR path] [options]
 * }}}
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
object DecisionTreeExample {

  case class Params(
      input: String = "../data/mllib/rf_libsvm_data.txt",
      testInput: String = "",
      dataFormat: String = "libsvm",
      algo: String = "Classification",//"regression",算法类型
      maxDepth: Int = 5,//最大深度
      maxBins: Int = 32,//连续特征离散化的最大数量,以及选择每个节点分裂特征的方式
      minInstancesPerNode: Int = 1,//分裂后自节点最少包含的实例数量
      minInfoGain: Double = 0.0,//分裂节点时所需最小信息增益
      fracTest: Double = 0.2,//测试因子
      cacheNodeIds: Boolean = false,
      checkpointDir: Option[String] = None,//检查点目录
      //设置检查点间隔(>=1),或不设置检查点(-1)
      checkpointInterval: Int = 10) extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("DecisionTreeExample") {
      head("DecisionTreeExample: an example decision tree app.")
      opt[String]("algo")
        .text(s"algorithm (classification, regression), default: ${defaultParams.algo}")
        .action((x, c) => c.copy(algo = x))
      opt[Int]("maxDepth")
        .text(s"max depth of the tree, default: ${defaultParams.maxDepth}")
        .action((x, c) => c.copy(maxDepth = x))
      opt[Int]("maxBins")
        .text(s"max number of bins, default: ${defaultParams.maxBins}")
        .action((x, c) => c.copy(maxBins = x))
      opt[Int]("minInstancesPerNode")
        .text(s"min number of instances required at child nodes to create the parent split," +
          s" default: ${defaultParams.minInstancesPerNode}")
        .action((x, c) => c.copy(minInstancesPerNode = x))
      opt[Double]("minInfoGain")//分裂节点时所需最小信息增益
        .text(s"min info gain required to create a split, default: ${defaultParams.minInfoGain}")
        .action((x, c) => c.copy(minInfoGain = x))
      opt[Double]("fracTest")
        .text(s"fraction of data to hold out for testing.  If given option testInput, " +
          s"this option is ignored. default: ${defaultParams.fracTest}")
        .action((x, c) => c.copy(fracTest = x))
      opt[Boolean]("cacheNodeIds")
        .text(s"whether to use node Id cache during training, " +
          s"default: ${defaultParams.cacheNodeIds}")
        .action((x, c) => c.copy(cacheNodeIds = x))
      opt[String]("checkpointDir")
        .text(s"checkpoint directory where intermediate node Id caches will be stored, " +
         s"default: ${defaultParams.checkpointDir match {
           case Some(strVal) => strVal
           case None => "None"
         }}")
        .action((x, c) => c.copy(checkpointDir = Some(x)))
      opt[Int]("checkpointInterval")//设置检查点间隔(>=1),或不设置检查点(-1)
        .text(s"how often to checkpoint the node Id cache, " +
         s"default: ${defaultParams.checkpointInterval}")//设置检查点间隔(>=1),或不设置检查点(-1)
        .action((x, c) => c.copy(checkpointInterval = x))//设置检查点间隔(>=1),或不设置检查点(-1)
      opt[String]("testInput")
        .text(s"input path to test dataset.  If given, option fracTest is ignored." +
          s" default: ${defaultParams.testInput}")
        .action((x, c) => c.copy(testInput = x))
      opt[String]("dataFormat")
        .text("data format: libsvm (default), dense (deprecated in Spark v1.1)")
        .action((x, c) => c.copy(dataFormat = x))
      //arg[String]("<input>")
       // .text("input path to labeled examples")
        //.required()
       // .action((x, c) => c.copy(input = x))
      checkConfig { params =>
        if (params.fracTest < 0 || params.fracTest >= 1) {
          failure(s"fracTest ${params.fracTest} value incorrect; should be in [0,1).")
        } else {
          success
        }
      }
    }

    parser.parse(args, defaultParams).map { params =>{
      //println(">>>>>>>>>")
      run(params)
      }
    }.getOrElse {
      sys.exit(1)
    }
  }

  /** 
   *  Load a dataset from the given path, using the given format
   *  从给定的路径加载数据集,使用给定的格式
   *   */
  private[ml] def loadData(
      sc: SparkContext,
      path: String,
      format: String,
      expectedNumFeatures: Option[Int] = None): RDD[LabeledPoint] = {
    format match {
    //LabeledPoint标记点是局部向量,向量可以是密集型或者稀疏型,每个向量会关联了一个标签(label)
      case "dense" => MLUtils.loadLabeledPoints(sc, path)
      case "libsvm" => expectedNumFeatures match {
        case Some(numFeatures) => MLUtils.loadLibSVMFile(sc, path, numFeatures)
      	/**
       	*  libSVM的数据格式
       	*  <label> <index1>:<value1> <index2>:<value2> ...
       	*  其中<label>是训练数据集的目标值,对于分类,它是标识某类的整数(支持多个类);对于回归,是任意实数
       	*  <index>是以1开始的整数,可以是不连续
       	*  <value>为实数,也就是我们常说的自变量
       	*/
        case None => MLUtils.loadLibSVMFile(sc, path)
      }
      case _ => throw new IllegalArgumentException(s"Bad data format: $format")
    }
  }

  /**
   * Load training and test data from files.
   * 从文件中加载训练和测试数据
   * @param input  Path to input dataset. 输入数据集的路径
   * @param dataFormat  "libsvm" or "dense"
   * @param testInput  Path to test dataset. 测试数据集的路径
   * @param algo  Classification or Regression 分类或回归
   * @param fracTest  Fraction of input data to hold out for testing.  Ignored if testInput given.
   * 				用于测试的输入数据的分数,如果testinput给忽略了
   * @return  (training dataset, test dataset)
   */
  private[ml] def loadDatasets(
      sc: SparkContext,
      input: String,
      dataFormat: String,
      testInput: String,
      algo: String,
      fracTest: Double): (DataFrame, DataFrame) = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // Load training data 加载 训练数据
    val origExamples: RDD[LabeledPoint] = loadData(sc, input, dataFormat)

    // Load or create test set 加载或创建测试集
    val splits: Array[RDD[LabeledPoint]] = if (testInput != "") {
      // Load testInput. 
      val numFeatures = origExamples.take(1)(0).features.size
     
      val origTestExamples: RDD[LabeledPoint] =
        loadData(sc, testInput, dataFormat, Some(numFeatures))
      Array(origExamples, origTestExamples)
    } else {
      // Split input into training, test. 将输入拆分为训练,测试
      origExamples.randomSplit(Array(1.0 - fracTest, fracTest), seed = 12345)
    }

    // For classification, convert labels to Strings since we will index them later with    
    // StringIndexer.
    //进行分类,因为我们将索引他们以后,将标签转换为字符串
    def labelsToStrings(data: DataFrame): DataFrame = {
      algo.toLowerCase match {
        case "classification" =>//如果算法分类,则添加列字段
          //withColumn函数就能实现对dataframe中列的添加,data列取字段"label"数据,强制转换字符号串类型
          data.withColumn("labelString", data("label").cast(StringType))
        case "regression" =>
          data
        case _ =>
          throw new IllegalArgumentException("Algo ${params.algo} not supported.")
      }
    }
    val dataframes = splits.map(_.toDF()).map(labelsToStrings)   
    val training = dataframes(0).cache()
    val test = dataframes(1).cache()

    val numTraining = training.count()//训练数据总数
    val numTest = test.count()//测试数据总数
   /** 
    +-----+--------------------+-----------+
    |label|            features|labelString|
    +-----+--------------------+-----------+
    |  0.0| (6,[4,5],[1.0,1.0])|        0.0|
    |  0.0|(6,[2,4,5],[1.0,1...|        0.0|
    |  0.0| (6,[4,5],[1.0,1.0])|        0.0|
    |  1.0|(6,[0,1,2,3],[1.0...|        1.0|
    |  1.0|(6,[1,2,3],[1.0,1...|        1.0|
    +-----+--------------------+-----------+**/
    training.show()
    //获取特征数
    val numFeatures = training.select("features").first().getAs[Vector](0).size
    //(6,[4,5],[1.0,1.0])
    val tset1=training.select("features").first().getAs[Vector](0)//(0)取1行数据
     println("==="+tset1)
    println("Loaded data:")
    //训练总数: numTraining = 5, 测试总数:numTest = 4
    println(s"  numTraining = $numTraining, numTest = $numTest")
    //特征数:numFeatures = 6
    println(s"  numFeatures = $numFeatures")
    (training, test)
  }

  def run(params: Params) {
    val conf = new SparkConf().setAppName(s"DecisionTreeExample with $params").setMaster("local[*]")
    val sc = new SparkContext(conf)
    params.checkpointDir.foreach(sc.setCheckpointDir)
    val algo = params.algo.toLowerCase

    println(s"DecisionTreeExample with parameters:\n$params")

    // Load training and test data and cache it. 加载训练和测试数据并将其缓存
    val (training: DataFrame, test: DataFrame) =
      loadDatasets(sc, params.input, params.dataFormat, params.testInput, algo, params.fracTest)

    // Set up Pipeline 建立管道
    //将特征转换,特征聚合,模型等组成一个管道,并调用它的fit方法拟合出模型*/  
    //一个 Pipeline 在结构上会包含一个或多个 PipelineStage,每一个 PipelineStage 都会完成一个任务
    val stages = new mutable.ArrayBuffer[PipelineStage]()
    // (1) For classification, re-index classes. 对于分类模型标签列名:indexedLabel,回归模型标签列名:label
    val labelColName = if (algo == "classification") "indexedLabel" else "label"
    if (algo == "classification") {
      val labelIndexer = new StringIndexer()
        .setInputCol("labelString")
        .setOutputCol(labelColName)
      stages += labelIndexer
    }
    // (2) Identify categorical features using VectorIndexer.
    //     确定使用vectorindexer分类特征
    //     Features with more than maxCategories values will be treated as continuous.
    //    VectorIndexer是对数据集特征向量中的类别(离散值)特征进行编号
    val featuresIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(10)
    stages += featuresIndexer
    // (3) Learn Decision Tree 学习决策树
    val dt = algo match {
      case "classification" =>//分类
        new DecisionTreeClassifier()
	 //训练数据集DataFrame中存储特征数据的列名
          .setFeaturesCol("indexedFeatures")//特征列
          .setLabelCol(labelColName)//标签列
          .setMaxDepth(params.maxDepth)//最大深度
          .setMaxBins(params.maxBins)//连续特征离散化的最大数量，以及选择每个节点分裂特征的方式
          .setMinInstancesPerNode(params.minInstancesPerNode)//分裂后自节点最少包含的实例数量
          .setMinInfoGain(params.minInfoGain)//分裂节点时所需最小信息增益
          .setCacheNodeIds(params.cacheNodeIds)//
          .setCheckpointInterval(params.checkpointInterval)//设置检查点间隔(>=1)
      case "regression" =>//回归
        new DecisionTreeRegressor()
          .setFeaturesCol("indexedFeatures")//特征列
          .setLabelCol(labelColName)//标签列
          .setMaxDepth(params.maxDepth)//最大深度
          .setMaxBins(params.maxBins)//连续特征离散化的最大数量
          .setMinInstancesPerNode(params.minInstancesPerNode)//分裂后自节点最少包含的实例数量
          .setMinInfoGain(params.minInfoGain)//分裂节点时所需最小信息增益
          .setCacheNodeIds(params.cacheNodeIds)
          .setCheckpointInterval(params.checkpointInterval)//设置检查点间隔(>=1)
      case _ => throw new IllegalArgumentException("Algo ${params.algo} not supported.")
    }
    stages += dt
     //PipeLine:将多个DataFrame和Estimator算法串成一个特定的ML Wolkflow
    //一个 Pipeline在结构上会包含一个或多个 PipelineStage,每一个 PipelineStage 都会完成一个任务
    val pipeline = new Pipeline().setStages(stages.toArray)

    // Fit the Pipeline 安装管道
    // 系统计时器的当前值,以毫微秒为单位
    val startTime = System.nanoTime()
    //fit()方法将DataFrame转化为一个Transformer的算法
    val pipelineModel = pipeline.fit(training)
    //1e9就为1*(10的九次方),也就是十亿
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    //打印训练使用的时间    
    println(s"Training time: $elapsedTime seconds")

    // Get the trained Decision Tree from the fitted PipelineModel
    //从拟合的管道模型中得到训练的决策树
    algo match {
      case "classification" =>//分类
        //获得管道中训练的最后模型决策树
        val treeModel = pipelineModel.stages.last.asInstanceOf[DecisionTreeClassificationModel]
        if (treeModel.numNodes < 20) {//节点数
          println(treeModel.toDebugString) // Print full model. 打印完整的模型
        } else {
          println(treeModel) // Print model summary. 打印模型综述
        }
      case "regression" =>//回归
        //获得管道模型中训练的决策树
        val treeModel = pipelineModel.stages.last.asInstanceOf[DecisionTreeRegressionModel]
        if (treeModel.numNodes < 20) {
          println(treeModel.toDebugString) // Print full model.
        } else {
          println(treeModel) // Print model summary.
        }
      case _ => throw new IllegalArgumentException("Algo ${params.algo} not supported.")
    }

    // Evaluate model on training, test data 训练评估模型，测试数据
    algo match {
      case "classification" =>//分类
        println("Training data results:")
        evaluateClassificationModel(pipelineModel, training, labelColName)
        println("Test data results:")
        evaluateClassificationModel(pipelineModel, test, labelColName)
      case "regression" =>//回归
        println("Training data results:")
        evaluateRegressionModel(pipelineModel, training, labelColName)
        println("Test data results:")
        evaluateRegressionModel(pipelineModel, test, labelColName)
      case _ =>
        throw new IllegalArgumentException("Algo ${params.algo} not supported.")
    }

    sc.stop()
  }

  /**
   * Evaluate the given ClassificationModel on data.  Print the results.
   * 评估给定的分类模型数据并打印结果
   * @param model  Must fit ClassificationModel abstraction 必须适合分类模型抽象
   * @param data  DataFrame with "prediction" and labelColName columns “预测”和labelcolname列数据框
   * @param labelColName  Name of the labelCol parameter for the model 该模型的labelcol参数名称
   *
   * TODO: Change model type to ClassificationModel once that API is public. SPARK-5995
   */
  private[ml] def evaluateClassificationModel(
      model: Transformer,
      data: DataFrame,
      labelColName: String): Unit = {
      //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val fullPredictions = model.transform(data).cache()
    //获得预测列
    /**
    +-----+--------------------+-----------+------------+--------------------+-------------+-----------+----------+
    |label|            features|labelString|indexedLabel|     indexedFeatures|rawPrediction|probability|prediction|
    +-----+--------------------+-----------+------------+--------------------+-------------+-----------+----------+
    |  1.0|(6,[0,1,2,3],[1.0...|        1.0|         1.0|(6,[0,1,2,3],[1.0...|    [0.0,2.0]|  [0.0,1.0]|       1.0|
    |  1.0|(6,[0,1,3],[1.0,1...|        1.0|         1.0|(6,[0,1,3],[1.0,1...|    [0.0,2.0]|  [0.0,1.0]|       1.0|
    |  0.0|(6,[0,4,5],[1.0,1...|        0.0|         0.0|(6,[0,4,5],[1.0,1...|    [3.0,0.0]|  [1.0,0.0]|       0.0|
    |  0.0|(6,[0,4,5],[1.0,1...|        0.0|         0.0|(6,[0,4,5],[1.0,1...|    [3.0,0.0]|  [1.0,0.0]|       0.0|
    +-----+--------------------+-----------+------------+--------------------+-------------+-----------+----------+**/
    fullPredictions.show(5)
    val predictions = fullPredictions.select("prediction").map(_.getDouble(0))
    //获得label列的值
    val labels = fullPredictions.select(labelColName).map(_.getDouble(0))
    // Print number of classes for reference
    /**
     * fullPredictions.schema("indexedLabel")
     * StructField(indexedLabel,DoubleType,true)||||indexedLabel
     */
    println(fullPredictions.schema(labelColName)+"||||"+labelColName)
    //getNumClasses 检查一个schema标识标签列中分类的数目
    val numClasses = MetadataUtils.getNumClasses(fullPredictions.schema(labelColName)) match {
      case Some(n) => n
      case None => throw new RuntimeException(
        //分类标签索引时出现未知故障
        "Unknown failure when indexing labels for classification.")
    }
    //(1.0,1.0)
    //(1.0,1.0)
    predictions.zip(labels).foreach(println _)
    //评估指标-多分类
    val accuracy = new MulticlassMetrics(predictions.zip(labels)).precision
    println(s"  Accuracy ($numClasses classes): $accuracy")
  }

  /**
   * Evaluate the given RegressionModel on data.  Print the results.
   * 评估给定回归模型数据并打印结果
   * @param model  Must fit RegressionModel abstraction 必须回归模型的抽象
   * @param data  DataFrame with "prediction" and labelColName columns 数据集包括prediction列名
   * @param labelColName  Name of the labelCol parameter for the model 该模型的标签列的参数名称
   *
   * TODO: Change model type to RegressionModel once that API is public. SPARK-5995
   */
  private[ml] def evaluateRegressionModel(
      model: Transformer,
      data: DataFrame,
      labelColName: String): Unit = {
      //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val fullPredictions = model.transform(data).cache()
    /***
     * 
     *+-----+--------------------+--------------------+----------+
      |label|            features|     indexedFeatures|prediction|
      +-----+--------------------+--------------------+----------+
      |  1.0|(6,[0,1,2,3],[1.0...|(6,[0,1,2,3],[1.0...|       1.0|
      |  1.0|(6,[0,1,3],[1.0,1...|(6,[0,1,3],[1.0,1...|       1.0|
      |  0.0|(6,[0,4,5],[1.0,1...|(6,[0,4,5],[1.0,1...|       0.0|
      |  0.0|(6,[0,4,5],[1.0,1...|(6,[0,4,5],[1.0,1...|       0.0|
      +-----+--------------------+--------------------+----------+
     */
    fullPredictions.show()    
    val predictions = fullPredictions.select("prediction").map(_.getDouble(0))
    val labels = fullPredictions.select(labelColName).map(_.getDouble(0))    
    //(1.0,1.0)
    //(1.0,1.0)
    val zip=predictions.zip(labels)    
   // println("zip:"+zip.)
    //RMSE 均方根误差
    val RMSE = new RegressionMetrics(predictions.zip(labels)).rootMeanSquaredError
    //Root mean squared error (RMSE): 0.0
    println(s"  Root mean squared error (RMSE): $RMSE")
  }
}
// scalastyle:on println
