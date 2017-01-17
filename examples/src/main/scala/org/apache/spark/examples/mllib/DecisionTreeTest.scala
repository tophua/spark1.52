package org.apache.spark.examples.mllib
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.impurity.Gini
/**
 * 决策树测试
 */
object DecisionTreeTest {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("KMeansClustering")
    val sc = new SparkContext(sparkConf)
    val data = sc.textFile("../data/mllib/sample_tree_data.csv")    
    val parsedData = data.map { line =>
      val parts = line.split(',').map(_.toDouble)
      //LabeledPoint标记点是局部向量,向量可以是密集型或者稀疏型,每个向量会关联了一个标签(label)
      LabeledPoint(parts(0), Vectors.dense(parts.tail))
    }

    val maxDepth = 5//树的最大深度，默认值是 5
    val model = DecisionTree.train(parsedData, Classification, Gini, maxDepth)

    val labelAndPreds = parsedData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val trainErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / parsedData.count
    println("Training Error = " + trainErr)
  }
}