package org.apache.spark.examples.mllib
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.mllib.clustering.{ KMeans, KMeansModel }
import org.apache.spark.mllib.linalg.Vectors
/**
 * http://www.ibm.com/developerworks/cn/opensource/os-cn-spark-practice4/index.html
 * 我们将根据目标客户的消费数据，将每一列视为一个特征指标，对数据集进行聚类分析(共8列)
 */
object KMeansClustering {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("KMeansClustering")
    val sc = new SparkContext(sparkConf)   
    /**
     * 
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
    val numClusters = 6//k 表示期望的聚类的个数
    val numIterations = 10 //表示方法单次运行最大的迭代次数
    val runTimes = 3 //表示算法被运行的次数,选出最优解
    var clusterIndex: Int = 0 
    //train方法对数据集进行聚类训练，这个方法会返回 KMeansModel 类实例
    val clusters: KMeansModel =
      KMeans.train(parsedTrainingData, numClusters, numIterations, runTimes)
     //聚类中心点
    println("Cluster Number:" + clusters.clusterCenters.length)
    println("Cluster Centers Information Overview:")
    //打印出中心点
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
     /*   line.split(",").map(_.trim).filter(!"".equals(_)).map{line=>
          println(line)
        }*/
        Vectors.dense(line.split(",").map(_.trim).filter(!"".equals(_)).map(_.toDouble))
      })
    parsedTestData.collect().foreach(testDataLine => {
      //计算测试数据分别属于那个簇类
      val predictedClusterIndex: Int = clusters.predict(testDataLine)
      println("测试样本: " + testDataLine.toString + " 属于聚类 " +
        predictedClusterIndex)
    })
    println("Spark MLlib K-means clustering test finished.")
    //评估KMeans模型 如何选择K值
    val ks: Array[Int] = Array(3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 50, 80, 100)
    ks.foreach(cluster => {
      //parsedTrainingData训练模型数据
      val model: KMeansModel = KMeans.train(parsedTrainingData, cluster, 30, 1)
      //KMeansModel 类里提供了 computeCost 方法，该方法通过计算所有数据点到其最近的中心点的平方和来评估聚类的效果。  
      //统计聚类错误的样本比例
      val ssd = model.computeCost(parsedTrainingData)
      //model.predict(point)
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