package org.apache.spark.examples.mllib
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.mllib.clustering.{ KMeans, KMeansModel }
import org.apache.spark.mllib.linalg.Vectors
/**
 * 我们将根据目标客户的消费数据，将每一列视为一个特征指标，对数据集进行聚类分析(共8列)
 */
object KMeansClustering {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("KMeansClustering")
    val sc = new SparkContext(sparkConf)   
    /**
     * Channel Region Fresh Milk Grocery Frozen Detergents_Paper Delicassen
     * 2 3
     * 12669 9656 7561 214 2674 1338
     * 2 3 7057 9810 9568 1762 3293 1776
     * 2 3 6353 8808
     * 7684 2405 3516 7844
     */
    val rawTrainingData = sc.textFile("../data/mllib/wholesale_customers_data_training.txt")
    //isColumnNameLine 过虑掉标题 
    val parsedTrainingData =
      rawTrainingData.filter(!isColumnNameLine(_)).map(line => {
        // println(">>>>>>>>>>>>" + line.split(",").map(_.trim).filter(!"".equals(_)))
        Vectors.dense(line.split(",").map(_.trim).filter(!"".equals(_)).map(_.toDouble))
      }).cache()
    // Cluster the data into two classes using KMeans
    val numClusters = 8 //k 表示期望的聚类的个数
    val numIterations = 30 //表示方法单次运行最大的迭代次数
    val runTimes = 3 //表示算法被运行的次数
    var clusterIndex: Int = 0 
    //train方法对数据集进行聚类训练，这个方法会返回 KMeansModel 类实例
    val clusters: KMeansModel =
      KMeans.train(parsedTrainingData, numClusters, numIterations, runTimes)
    println("Cluster Number:" + clusters.clusterCenters.length)
    println("Cluster Centers Information Overview:")
    clusters.clusterCenters.foreach(
      x => {
        println("Center Point of Cluster " + clusterIndex + ":")
        println(x)
        clusterIndex += 1
      })
    //begin to check which cluster each test data belongs to based on the clustering result
    val rawTestData = sc.textFile("../data/mllib/wholesale_customers_data_test.txt")
    val parsedTestData = rawTestData.filter(!isColumnNameLine(_)).map(line =>
      {
        //打印每列值
        line.split(",").map(_.trim).filter(!"".equals(_)).map{line=>
          println(line)
        }
        Vectors.dense(line.split(",").map(_.trim).filter(!"".equals(_)).map(_.toDouble))
      })
    parsedTestData.collect().foreach(testDataLine => {
      //对新的数据点进行所属聚类的预测
      val predictedClusterIndex: Int = clusters.predict(testDataLine)
      println("The data " + testDataLine.toString + " belongs to cluster " +
        predictedClusterIndex)
    })
    println("Spark MLlib K-means clustering test finished.")
    //如何选择K值
    val ks: Array[Int] = Array(3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 50, 80, 100)
    ks.foreach(cluster => {
      val model: KMeansModel = KMeans.train(parsedTrainingData, cluster, 30, 1)
      val ssd = model.computeCost(parsedTrainingData)
      println("sum of squared distances of points to their nearest center when k=" + cluster + " -> " + ssd)
    })
  }
  //过滤标题行
  private def isColumnNameLine(line: String): Boolean = {
    if (line != null &&
      line.contains("Channel")) true
    else false
  }
}