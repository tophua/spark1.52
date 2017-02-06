package org.apache.spark.examples.mllib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
/**
 * Spark coolbook p163
 */
object ALSDome {
  def main(args: Array[String]) {
    /**
     * 协同过滤ALS算法推荐过程如下：
     * 加载数据到 ratings RDD,每行记录包括：user, product, rate
     * 从 ratings 得到用户商品的数据集：(user, product)
     * 使用ALS对 ratings 进行训练
     * 通过 model 对用户商品进行预测评分：((user, product), rate)
     * 从 ratings 得到用户商品的实际评分：((user, product), rate)
     * 合并预测评分和实际评分的两个数据集,并求均方差
     */
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("ALSExample")
    val sc = new SparkContext(sparkConf)
    /**文件中每一行包括一个用户id、商品id和评分****/
    val data = sc.textFile("../data/mllib/u.data")
    //将val数据转换(transform)到评分(Rating)RDD
    val ratings = data.map { line =>
        val Array(userId, itemId, rating, _) = line.split("\t")
        //用户ID,产品ID,(评级,等级)
        Rating(userId.toInt, itemId.toInt, rating.toDouble)
    }
    
    //将个人评分数据导入RDD
    /**
     * 用户ID,电影ID,评分
     * 944,313,5
       944,2,3
       944,1,1
       944,43,4
       944,67,4
       944,82,5
       944,96,5
       944,121,4
       944,148,4
     */
    val pdata = sc.textFile("../data/mllib/p.data")
    val pratings = pdata.map { line =>
        val Array(userId, itemId, rating) = line.split(",")
        println(userId.toInt+"||"+itemId.toInt+"||"+rating.toDouble)
         /**文件中每一行包括一个用户id、商品id和评分****/
        Rating(userId.toInt, itemId.toInt, rating.toDouble)
       }
   //绑定评分数据和个人评分数据
    val movieratings = ratings.union(pratings)
    //使用ALS建立模型,设定rank为5,迭代次数为10以及lambda为0.01
    val model = ALS.train(movieratings, 10, 10, 0.01)
     //在模型上选定一部电影预测我的评分,让我们从电影ID为195的<终结者>开始
    model.predict(sc.parallelize(Array((944,195)))).collect.foreach(println)
    //在模型上选定一部电影预测我的评分,让我们从电影ID为402<人鬼情未了>
    model.predict(sc.parallelize(Array((944,402)))).collect.foreach(println)
    //在模型上选定一部电影预测我的评分,让我们从电影ID为148<黑夜幽灵>
    model.predict(sc.parallelize(Array((944,402)))).collect.foreach(println)  
  }
}