package org.apache.spark.examples

import scala.reflect.runtime.universe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.IDF
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.Row

/**
 *使用Spark MLlib提供的朴素贝叶斯(Native Bayes)算法,完成对中文文本的分类过程。
 * 主要包括中文分词、文本表示（TF-IDF）、模型训练、分类预测等
 * http://lxw1234.com/archives/2016/01/605.htm
 */
case class RawDataRecord(category: String, text: String)
object NativeBayesHashingTF {
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[2]").setAppName("NativeBayes")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //隐式导入,自动转换toDF
    import sqlContext.implicits._
    var srcRDD = sc.textFile("../data/mllib/sougou/C000007/").filter(!_.isEmpty).map {
      x =>
        // println("==="+x+"===========")
        var data = x.split(",")
        println("==="+data(0)+"\t======"+data(1))
        RawDataRecord(data(0), data(1))
    }
    //70%作为训练数据,30%作为测试数据
    val splits = srcRDD.randomSplit(Array(0.7, 0.3))
    var trainingDF = splits(0).toDF()
    var testDF = splits(1).toDF()
    //将词语转换成数组
    var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    var wordsData = tokenizer.transform(trainingDF)
    //output1：（将词语转换成数组）
    wordsData.show()
    println("output1：")
    /**
     *+--------------------+--------------------+--------------------+
      |            category|                text|               words|
      +--------------------+--------------------+--------------------+
      |在政府部门明确表示汽车投资过热...|日前新飞集团却逆流而上：专用汽车工...|[日前新飞集团却逆流而上：专用汽车...|
      +--------------------+--------------------+--------------------+
     */
    wordsData.select($"category", $"text", $"words").show(1)
    //wordsData.select($"category", $"text", $"words").take(1)
   //将每个词转换成Int型,并计算其在文档中的词频(TF)
    var hashingTF = new HashingTF().setNumFeatures(500000).setInputCol("words").setOutputCol("rawFeatures")
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    var featurizedData = hashingTF.transform(wordsData)
    //output2：（计算每个词在文档中的词频）
    println("output2：")
    /**
    +--------------------+--------------------+--------------------+
    |            category|               words|         rawFeatures|
    +--------------------+--------------------+--------------------+
    |在政府部门明确表示汽车投资过热...|[日前新飞集团却逆流而上：专用汽车...|(500000,[372412],...|
    +--------------------+--------------------+--------------------+
     */
    featurizedData.select($"category", $"words", $"rawFeatures").show()
    //println(">>>>>>>>>>>>>>>."+featurizedData.toString())
    //逆文档频率(IDF),用来衡量一个词语特定文档的相关度,计算TF-IDF值
    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    //fit()方法将DataFrame转化为一个Transformer的算法
    var idfModel = idf.fit(featurizedData)
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    var rescaledData = idfModel.transform(featurizedData)
    //output3：（计算每个词的TF-IDF）
    println("output3：")
    /**
    +--------------------+--------------------+
    |            category|            features|
    +--------------------+--------------------+
          在政府部门明确表示汽车投资过热...|(500000,[372412],...|
    +--------------------+--------------------+
     */
    rescaledData.select($"category", $"features").show()
    /**
     * 将上面的数据转换成Bayes算法需要的格式:
     		0,1 0 0
        0,2 0 0
        1,0 1 0
        1,0 2 0
        2,0 0 1
        2,0 0 2
     */
    var trainDataRdd = rescaledData.select($"category", $"features").map {
      case Row(label: String, features: Vector) =>
      //LabeledPoint标记点是局部向量,向量可以是密集型或者稀疏型,每个向量会关联了一个标签(label)
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }
    //output4:(Bayes算法的输入数据格式)
    println("output4：")
    trainDataRdd.take(1)

    
    var srcDF = sc.textFile("../data/mllib/1.txt").map { 
      x => 
        var data = x.split(",")
        RawDataRecord(data(0),data(1))
    }.toDF()

    //训练模型,modelType模型类型(区分大小写)multinomial多分类
    val model = NaiveBayes.train(trainDataRdd, lambda = 1.0, modelType = "multinomial")
    //测试数据集,做同样的特征表示及格式转换
    var testwordsData = tokenizer.transform(testDF)
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    var testfeaturizedData = hashingTF.transform(testwordsData)
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    var testrescaledData = idfModel.transform(testfeaturizedData)
    /**
     * 将上面的数据转换成Bayes算法需要的格式:
     		0,1 0 0
        0,2 0 0
        1,0 1 0
        1,0 2 0
        2,0 0 1
        2,0 0 2
     */
    var testDataRdd = testrescaledData.select($"category", $"features").map {
      case Row(label: String, features: Vector) =>
      //LabeledPoint标记点是局部向量,向量可以是密集型或者稀疏型,每个向量会关联了一个标签(label)
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }

    //对测试数据集使用训练模型进行分类预测
    val testpredictionAndLabel = testDataRdd.map(p => (model.predict(p.features), p.label))

    //统计分类准确率
    var testaccuracy = 1.0 * testpredictionAndLabel.filter(x => x._1 == x._2).count() / testDataRdd.count()
    //output5：（测试数据集分类准确率）
    println("output5：")
   //准确率90%,还可以。接下来需要收集分类更细,时间更新的数据来训练和测试了。 
    println(testaccuracy)

  }
}
