package org.apache.spark.examples.ml

import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.examples.mllib.AbstractParams
import org.apache.spark.ml.{ Pipeline, PipelineStage }
import org.apache.spark.ml.classification.{ LogisticRegression, LogisticRegressionModel }
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.DataFrame
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.classification.LogisticRegression

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.util.Utils

   case class Feature(v: Vector)
  object LogisticRegressionExamples {
/**
 * Spark Coolbook 使用ML创建机器学习流水线
 * ML基本概念:它使用转换器将一个DataFrame转换为另一个DataFrame,
 * 一个转换器的简单例子就是增加一列,你可以等价于关系数据库的"alter table"
 * 估算使用训练集数据训练算法,然后转换器作出预测
 */
  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("CountVectorizerExample").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    import sqlContext.implicits._
    //创建一个名叫Lebron的篮球运动员的向量标签 80高,250重
    val lebron = LabeledPoint(1.0, Vectors.dense(80.0, 250.0))
    //创建一个名叫tim的篮球运动员的向量标签 70高,150重
    val tim = LabeledPoint(0.0, Vectors.dense(70.0, 150.0))
    //创建一个名叫brittan的篮球运动员的向量标签 80高,207重
    val brittany = LabeledPoint(1.0, Vectors.dense(80.0, 207.0))
    //创建一个名叫Stacey的篮球运动员的向量标签 65高,120重
    val stacey = LabeledPoint(0.0, Vectors.dense(65.0, 120.0))    
    //创建一个训练集RDD    
    val trainingRDD = sc.parallelize(List(lebron, tim, brittany, stacey))
    //创建一个训练集DataFrame
   val  trainingDF=sqlContext.createDataFrame(trainingRDD)
    //val trainingDF = sqlContext.toDF()
    /** 
     * 估算(estimator):代表着机器学习算法,即从机器中学习,估算的输入一个DataFrame,输出是一个转换器
     * 每个估算都有一个fit()方法,用于训练算法
     * 创建一个LogisticRegression估算
     */
    val estimator = new LogisticRegression
    //创建一个训练集DataFrame转换器
    //fit()方法将DataFrame转化为一个Transformer的算法
    val transformer = estimator.fit(trainingDF)
    //创建测试数据john身高90,体重270.0是篮球运动员
    val john = Vectors.dense(90.0, 270.0)
    //创建测试数据tom身高62,体重120.0不是篮球运动员
    val tom = Vectors.dense(62.0, 120.0)
    //创建训练集RDD
    val testRDD =  sc.parallelize(List(john, tom))
    //映射testRDD和Feature RDD
    val featuresRDD = testRDD.map(v => Feature(v))
    //将featuresRDD转换为列名features的DataFrame
    val featuresDF = featuresRDD.toDF("features")
    //在转换器增加预测列features,转换器作出预测
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val predictionsDF = transformer.transform(featuresDF)
    println("======")
    predictionsDF.foreach { x =>println _}
    //在predictionsDF增加了3列,行预测,可能性和预测,我们只选择特征性和预测列
    val shorterPredictionsDF = predictionsDF.select("features", "prediction")
    //将预测列(prediction)重名为isBasketBallPlayer
    val playerDF = shorterPredictionsDF.toDF("features", "isBasketBallPlayer")
    playerDF.foreach { x =>println _ }
    /**
     *[[62.0,120.0],[31.460769106353915,-31.460769106353915],[0.9999999999999782,2.1715087350282438E-14],0.0]
		 *[[90.0,270.0],[-61.88475862618768,61.88475862618768],[1.3298137364599064E-27,1.0],1.0]
     */
    playerDF.collect().foreach { x =>println _}
    //打印playerDF数据结构
    playerDF.printSchema
    
  }
}