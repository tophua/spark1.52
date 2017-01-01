package org.apache.spark.examples.mllib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
/**
 * 聚类算法:是把数据划分成多个组,其中一个组的数据与其他组的数据相似
 * 监督学习用标记好的数据去训练算法,无监督学习让算法自己去找内部结构
 * 使用加州市的房屋数据占地面积和房屋价格
 * 占地面积|房屋价格
 * 12839	 |2405
 * 10000	 |2200
 * 8040		 |1400
 * 13104	 |1800
 * 10000	 |2351
 * 3049		 |795
 * 38768	 |2725
 * 16250	 |2150
 * 43026	 |2724
 * 44431	 |2675
 */
object KMeansExample {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("KMeansClustering")
    val sc = new SparkContext(sparkConf)
    //加载saratoga到RDD
    val data = sc.textFile("../data/mllib/saratoga.csv")
    //把数据转换成密集向量的RDD
   val parsedData = data.map( line => Vectors.dense(line.split(',').map(_.toDouble)))
   //以4个簇和5次迭代训练模型
   val kmmodel= KMeans.train(parsedData,4,5)
   //把parsedData数据收集本地数据集
   val houses = parsedData.collect
   //预测第1个元素的簇,KMeans算法会从0给出簇的ID,
   val prediction1 = kmmodel.predict(houses(0))
   //预测houses(18)的数据,占地面积876,价格66.5属于那个簇
   val prediction2 = kmmodel.predict(houses(18))
   //预测houses(35)的数据,占地面积15750,价格112属于那个簇
   val prediction3 = kmmodel.predict(houses(35))
   //预测houses(6)的数据,占地面积38768,价格272属于那个簇
   val prediction4 = kmmodel.predict(houses(6))
   //预测houses(15)的数据,占地面积69696,价格275属于那个簇
   val prediction5 = kmmodel.predict(houses(15))
    
  }
 
}