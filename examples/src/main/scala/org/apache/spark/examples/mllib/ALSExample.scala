package org.apache.spark.examples.mllib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

object ALSExample {
  def main(args: Array[String]) {
    /**
     * 协同过滤ALS算法推荐过程如下：
     * 加载数据到 ratings RDD,每行记录包括：user, product, rate
     * 从 ratings 得到用户商品的数据集：(user, product)
     * 使用ALS对 ratings 进行训练
     * 通过 model 对用户商品进行预测评分：((user, product), rate)
     * 从 ratings 得到用户商品的实际评分：((user, product), rate)
     * 合并预测评分和实际评分的两个数据集，并求均方差
     */
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkHdfsLR")
    val sc = new SparkContext(sparkConf)
    /**文件中每一行包括一个用户id、商品id和评分****/
    val data = sc.textFile("../data/mllib/als/test.data")
    //匹配类型
    val ratings = data.map(_.split(',') match {
      case Array(user, product, rate) =>
        //用户ID,产品ID,(评级,等级)
        Rating(user.toInt, product.toInt, rate.toDouble)
    })
    //使用ALS训练数据建立推荐模型
    val rank = 10 //模型中隐语义因子的个数(隐分类模型),ALS中因子的个数，通常来说越大越好，但是对内存占用率有直接影响，通常rank在10到200之间
    val numIterations = 20 //迭代数
    val model = ALS.train(ratings, rank, numIterations, 0.01)
    //从 ratings 中获得只包含用户和商品的数据集 
    val usersProducts = ratings.map {
      case Rating(user, product, rate) =>
        (user, product)
    }
    //使用推荐模型对用户商品进行预测评分，得到预测评分的数据集
    val predictions =
      model.predict(usersProducts).map {
        case Rating(user, product, rate) =>
          ((user, product), rate)
      }
    //将真实评分数据集与预测评分数据集进行合并
    val ratesAndPreds = ratings.map {
      case Rating(user, product, rate) =>
        ((user, product), rate)
    }.join(predictions).sortByKey() //ascending or descending 
    //然后计算均方差，注意这里没有调用 math.sqrt方法
    val MSE = ratesAndPreds.map {
      case ((user, product), (r1, r2)) =>
        val err = (r1 - r2)
        err * err
    }.mean()
    //打印出均方差值  
    println("Mean Squared Error = " + MSE)
    //Mean Squared Error = 1.37797097094789E-5

    /***用户推荐商品**/

    //为每个用户进行推荐，推荐的结果可以以用户id为key，结果为value存入redis或者hbase中
    val users = data.map(_.split(",") match {
      case Array(user, product, rate) => (user)
    }).distinct().collect()
    //users: Array[String] = Array(4, 2, 3, 1)
    users.foreach(
      user => {
        //依次为用户推荐商品   
        var rs = model.recommendProducts(user.toInt, numIterations)
        var value = ""
        var key = 0

        //拼接推荐结果
        rs.foreach(r => {
          key = r.user
          value = value + r.product + ":" + r.rating + ","
        })
        println(key.toString + "   " + value)
      })

    //对预测结果按预测的评分排序
    predictions.collect.sortBy(_._2)
    //对预测结果按用户进行分组，然后合并推荐结果，这部分代码待修正
    predictions.map { case ((user, product), rate) => (user, (product, rate)) }.groupByKey.collect
    //格式化测试评分和实际评分的结果
    val formatedRatesAndPreds = ratesAndPreds.map {
      case ((user, product), (rate, pred)) => user + "," + product + "," + rate + "," + pred
    }
    formatedRatesAndPreds.collect()
  }
}