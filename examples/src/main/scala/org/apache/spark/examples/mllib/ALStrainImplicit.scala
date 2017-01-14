package org.apache.spark.examples.mllib
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
/**
 * 隐性反馈的协同过虑
 */
object ALStrainImplicit {
  def main(args: Array[String]) {
    /**
     * 协同过滤ALS算法推荐过程如下：
     * 加载数据到 ratings RDD,每行记录包括:user,product,rate
     * 从 ratings 得到用户商品的数据集:(user, product)
     * 使用ALS对 ratings 进行训练
     * 通过 model 对用户商品进行预测评分:((user, product),rate)
     * 从 ratings 得到用户商品的实际评分:((user, product),rate)
     * 合并预测评分和实际评分的两个数据集，并求均方差
     */
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkHdfsLR")
    val sc = new SparkContext(sparkConf)
    /**文件中每一行包括一个用户id、商品id和评分****/
    //将kaggle_songs数据导入到RDD
    val songs = sc.textFile("../data/mllib/als/kaggle_songs.txt")
    //将kaggle_users数据导入到RDD
    val users = sc.textFile("../data/mllib/als/kaggle_users.txt")
    //将kaggle_visible_evaluation_triplets数据导入到RDD
    val triplets = sc.textFile("../data/mllib/als/kaggle_visible_evaluation_triplets.txt")
    //将歌曲数据转换为PairRDD
    val songIndex = songs.map(_.split("\\W+")).map(v =>(v(0),v(1).toInt))
    //将songIndex转换为map数组
     /**
     * Map(SOWZAXW12A8C13FDF2 -> 345316, 
        SOJPVJN12A6BD4FE30 -> 152438, 
        SOCQGQQ12AB01899D1 -> 42237, 
        SOAZZNE12AB0186B41 -> 16112, 
        SOKVRTV12AC4689943 -> 171148, 
        SOWTILD12A6D4FA5CA -> 342308, 
        SOHEABL12A58A80C21 -> 114376, 
        SOEKOOE12A8C13FE18 -> 70864, 
        SODRTXS12A8C13CC3F -> 59311)
     */
    val songMap = songIndex.collectAsMap
    //将用户数据转换为PairRDD
    val userIndex = users.zipWithIndex.map( t => (t._1,t._2.toInt))
    //将UserIndex转换为Map数组
    /**
     * Map(af42f25800f70fe00eb91e6d5a7215493e1386cb -> 58435, 
       d54d311bcbdb6486ef17cc22414e5ce9a7f4834e -> 64818, 
       3ed4de747450da358ef70aee9da59c35f87535ef -> 21807, 
       159d56cc42ee8c91de32f7e7fcfaea77d486f9d9 -> 55694, 
       9b9fb6acabe8bdde3012ddc21626c435e62bc077 -> 57964, 
       a8f1947bf5122527228d6780535767657f679318 -> 99490, 
       07c040fbe93d6a56b316ff3383d046e80f9dd511 -> 95351, 
       77df6308c2ff6b24f1401106244b044db2e026a2 -> 319, 
       40420d1b02b2f590fc28fee7cf58da7c1dab7e40 -> 89248, 
       9475c8039e9bab7221b57ab23db7ccb4193ff6c7 -> 14820)
     */
    val userMap = userIndex.collectAsMap
    //广播userMap
    val broadcastUserMap = sc.broadcast(userMap)
    //广播songMap
    val broadcastSongMap = sc.broadcast(songMap)
    //将triplets数据转换为一个数组
    val tripArray = triplets.map(_.split("\\W+"))
    //导入Rating包
    import org.apache.spark.mllib.recommendation.Rating
    //将tripArray数组转换为评级对象RDD
    val ratings = tripArray.map { case Array(user, song, plays)=>
    val userId = broadcastUserMap.value.getOrElse(user, 0)
    val songId = broadcastUserMap.value.getOrElse(song, 0)
    Rating(userId, songId, plays.toDouble)
    }
    //导入ALS
    import org.apache.spark.mllib.recommendation.ALS
    //将Rank设置为10,迭代次数设为10,Rank模型中的潜在特征数    
    val model = ALS.trainImplicit(ratings, 10, 10)
    //从triplet中导出用户和歌曲元组
    val usersSongs = ratings.map( r => {
      println(r.user+"|||"+r.product)
      (r.user, r.product) 
    })
  

    //预测用户和歌曲
    val predictions = model.predict(usersSongs)
    predictions.foreach { x => println(x.user.toString()+"|||||"+x.rating.toString()+"======="+x.product.toString()) }
  }
}