package org.apache.spark.examples.sql

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
 * 比如，训练语料/tmp/lxw1234/1.txt：
 * 0,苹果 官网 苹果 宣布
 * 1,苹果 梨 香蕉
 */
case class RawDataRecord(category: String, text: String)

object NativeBayesSQL {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("NativeBayesSQL")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    //将原始数据映射到DataFrame中，字段category为分类编号，字段text为分好的词，以空格分隔
    var srcDF = sc.textFile("/tmp/lxw1234/1.txt").map {
      x =>
        var data = x.split(",")
        RawDataRecord(data(0), data(1))
    }.toDF()
    srcDF.select("category", "text").take(2).foreach(println)
    //[0,苹果 官网 苹果 宣布]
    //[1,苹果 梨 香蕉]
    //将分好的词转换为数组
    var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    var wordsData = tokenizer.transform(srcDF)
    wordsData.select($"category", $"text", $"words").take(2).foreach(println)
    //[0,苹果 官网 苹果 宣布,WrappedArray(苹果, 官网, 苹果, 宣布)]
    //[1,苹果 梨 香蕉,WrappedArray(苹果, 梨, 香蕉)]

    //将每个词转换成Int型，并计算其在文档中的词频（TF）
    var hashingTF =
     //setNumFeatures(100)表示将Hash分桶的数量设置为100个,这个值默认为2的20次方，即1048576，可以根据你的词语数量来调整，
     //一般来说，这个值越大，不同的词被计算为一个Hash值的概率就越小，数据也更准确，但需要消耗更大的内存
      new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(100)
    var featurizedData = hashingTF.transform(wordsData)
    featurizedData.select($"category", $"words", $"rawFeatures").take(2).foreach(println)
    //[0,WrappedArray(苹果, 官网, 苹果, 宣布),(100,[23,81,96],[2.0,1.0,1.0])]
    //[1,WrappedArray(苹果, 梨, 香蕉),(100,[23,72,92],[1.0,1.0,1.0])]
    //结果中，“苹果”用23来表示，第一个文档中，词频为2，第二个文档中词频为1.

    //计算TF-IDF值
    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    var idfModel = idf.fit(featurizedData)
    var rescaledData = idfModel.transform(featurizedData)
    rescaledData.select($"category", $"words", $"features").take(2).foreach(println)

    //[0,WrappedArray(苹果, 官网, 苹果, 宣布),(100,[23,81,96],[0.0,0.4054651081081644,0.4054651081081644])]
    //[1,WrappedArray(苹果, 梨, 香蕉),(100,[23,72,92],[0.0,0.4054651081081644,0.4054651081081644])]
    //因为一共只有两个文档，且都出现了“苹果”，因此该词的TF-IDF值为0.

    //每一个LabeledPoint中，特征数组的长度为100（setNumFeatures(100)），
    //”官网”和”宣布”对应的特征索引号分别为81和96，因此，在特征数组中，第81位和第96位分别为它们的TF-IDF值。


    var trainDataRdd = rescaledData.select($"category", $"features").map {
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }

  }
}