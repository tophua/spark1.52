package org.apache.spark.examples.mllib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.mllib.feature.Word2Vec

object Word2VecExample {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Word2VecExample")
    val sc = new SparkContext(sparkConf)
    val input = sc.textFile("../data/mllib/text8").map(line => line.split(" ").toSeq)

    val word2vec = new Word2Vec()
    //fit()方法将DataFrame转化为一个Transformer的算法
    val model = word2vec.fit(input)

    val synonyms = model.findSynonyms("china", 40)

    for ((synonym, cosineSimilarity) <- synonyms) {
      //相似性得分
      println(s"$synonym $cosineSimilarity")
    }

    // Save and load model
    //model.save(sc, "myModelPath")
    //val sameModel = Word2VecModel.load(sc, "myModelPath")

  }
}
