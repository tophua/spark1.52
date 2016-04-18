package org.apache.spark.examples.sparkGuide

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
object Quickstart {
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local").setAppName("Spark Exercise:Average Age Calculator")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile("README.md")
    val count = textFile.count()
    val first = textFile.first()
    val linesWithSpark = textFile.filter(line => line.contains("Spark"))
    linesWithSpark.foreach { x => println _ }

    val filterCount = textFile.filter(line => line.contains("Spark")).count()
    textFile.map(line => line.split(" ").size).foreach { x => println _ }
    val max = textFile.map(line => line.split(" ").size).reduce((a, b) => if (a > b) a else b)
    val Mathmax = textFile.map(line => line.split(" ").size).reduce((a, b) => Math.max(a, b))
    //reduceByKey对Key相同的元素的值求和
    val wordCounts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_+_)
    //collect将RDD转成Scala数组，并返回
    wordCounts.collect()
    linesWithSpark.cache()
    linesWithSpark.count()
    linesWithSpark.count()

  }

}