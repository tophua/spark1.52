package org.apache.spark.examples.mllib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
/**
 * 分层采样（Stratified sampling）
 * 本小节使用spark自带的README.md文件进行相应的演示操作
 */
object StratifiedSampleDemo {
  def main(args: Array[String]) {
    //val sparkConf = new SparkConf().setMast("local[2]").setAppName("SparkHdfsLR")

    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    //读取HDFS上的README.md文件
    val textFile = sc.textFile("/README.md")
    //wordCount操作,返回（K,V)汇总结果
    val wordCounts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)

    //定义key为spark,采样比率为0.5
    val fractions: Map[String, Double] = Map("Spark" -> 0.5)

    //使用sampleByKey方法进行采样
    val approxSample = wordCounts.sampleByKey(false, fractions)
    //使用sampleByKeyExact方法进行采样,该方法资源消耗较sampleByKey更大
    //但采样后的大小与预期大小更接近,可信度达到99.99%
    val exactSample = wordCounts.sampleByKeyExact(false, fractions)
  }
}