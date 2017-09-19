package org.apache.spark.examples.mllib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
/**
 * 聚类算法
 * http://blog.selfup.cn/728.html,
 * 将一组目标object划分为若干个簇,每个簇之间的object尽可能的相似,簇与簇之间的object尽可能的相异
 */
object KMeansDome {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("KMeansClustering")
    val sc = new SparkContext(sparkConf)
    val rawTrainingData = sc.textFile("../data/mllib/kmeans_data2.txt")
    val parsedData =
      rawTrainingData.map(line => {
        Vectors.dense(line.split(" ").map(_.trim).filter(!"".equals(_)).map(_.toDouble))
      }).cache()

    val pointsData = Seq(
      /***======聚中心点列相加除3============**/
      Vectors.dense(0.0, 0.0, 0.0),
      Vectors.dense(0.1, 0.1, 0.1),
      Vectors.dense(0.2, 0.2, 0.2),
      /***======聚中心点列相加除3============**/
      Vectors.dense(9.0, 9.0, 9.0),
      Vectors.dense(9.1, 9.1, 9.1),
      Vectors.dense(9.2, 9.2, 9.2),
      /***======聚中心点列相加除3============**/
      Vectors.dense(15.1, 16.1, 17.0),
      Vectors.dense(18.0, 17.0, 19.0),
      Vectors.dense(20.0, 21.0, 22.0))
    val parsedDataRdd = sc.parallelize(pointsData, 3)

    // Cluster the data into two classes using KMeans
    val numClusters = 3 //预测分为3个簇类
    val numIterations = 20 //迭代20次
    val runTimes = 10 //运行10次,选出最优解
    var clusterIndex: Int = 0
    //train方法对数据集进行聚类训练,这个方法会返回 KMeansModel 类实例
    val clusters: KMeansModel =
      KMeans.train(parsedData, numClusters, numIterations, runTimes)
      
     
     
    //计算测试数据分别属于那个簇类
    parsedData.map(v =>
      {
	//predict 对新的数据点进行所属聚类的预测
        println(v.toString() + " belong to cluster :" + clusters.predict(v))
      }).collect()
    //计算cost
     /**
      * computeCost通过计算所有数据点到其最近的中心点的平方和来评估聚类的效果,
      * 统计聚类错误的样本比例
      */
    val wssse = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + wssse)
    //打印出中心点
    /**
              中心点(Cluster centers):
 			[17.7,18.03333333333333,19.333333333333332]
 			[0.1,0.1,0.1]
 			[9.1,9.1,9.1]
     */
    println("中心点(Cluster centers):")
    for (center <- clusters.clusterCenters) {
      println(" " + center);
    }
    val points = Seq(
      Vectors.dense(1.1, 2.1, 3.1),
      Vectors.dense(10.1, 9.1, 11.1),
      Vectors.dense(21.1, 17.1, 16.1))
    val rdd = sc.parallelize(points, 3)

    val data1 = sc.parallelize(Array(Vectors.dense(1.1, 2.1, 3.1)))
    val data2 = sc.parallelize(Array(Vectors.dense(10.1, 9.1, 11.1)))
    val data3 = sc.parallelize(Array(Vectors.dense(21.1, 17.1, 16.1)))

    val pointsdens = clusters.predict(rdd).collect()
    
    //预测聚类类别
    for (p <- pointsdens) {
      println(p)

    }
    //进行一些预测
    val l = clusters.predict(data1).collect()
    val ll = clusters.predict(data2).collect()
    val lll = clusters.predict(data3).collect()
    /*   for(a<-l){
       println(a)
     }*/
    println("Prediction of (1.1, 2.1, 3.1): " + l(0))
    println("Prediction of (10.1, 9.1, 11.1): " + ll(0))
    println("Prediction of (21.1, 17.1, 16.1): " + lll(0))
     import breeze.linalg._
    import breeze.numerics.pow
    //定义欧拉距离之和,矩阵平方差之和
    def computeDistance(v1: DenseVector[Double], v2: DenseVector[Double]): Double = pow(v1 - v2, 2).sum //平方差之和

  }
 
}
