package org.apache.spark.examples.mllib
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.impurity.Entropy
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
/**
 * Spark coolbook p131
 * 决策树
 */
object DecisionTreeExample {
   def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("DecisionTreeExample").setMaster("local[4]")
    val sc = new SparkContext(conf)
    /**
     * 创建以病态数据体作为标记的LabledPoint数组
     */   
    /**
     * 下雨是否(2,1)
     * 风大是否(2,1)
     * 温度高,正常,冷(3,2,1)
     *是否打网球|下雨|风大|温度| 
      0.0,			1.0,	1.0,2.0
      0.0,			1.0,	1.0,1.0
      0.0,			1.0,	1.0,0.0
      0.0,			0.0,	1.0,2.0
      0.0,			0.0,	1.0,0.0
      1.0,			0.0,	0.0,2.0
      1.0,			0.0,	0.0,1.0
      0.0,			0.0,	0.0,0.0 
     */
    //加载文件
    val data = sc.textFile("../data/mllib/tennis.csv")
    //解析数据并把它加载到LablePoint
    val parsedData = data.map {line => 
          val parts = line.split(',').map(_.toDouble)
	  //LabeledPoint标记点是局部向量,向量可以是密集型或者稀疏型,每个向量会关联了一个标签(label)
          LabeledPoint(parts(0), Vectors.dense(parts.tail))
          }
    //用这些数据训练算法
   val model = DecisionTree.train(parsedData, Classification,Entropy, 3)
    //创建一个向量表示无雨,风大,低温
   val v=Vectors.dense(0.0,1.0,0.0)
   //预测是否打网球
   model.predict(v)
  
  }
}
