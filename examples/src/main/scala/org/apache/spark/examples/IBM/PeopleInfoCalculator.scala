package org.apache.spark.examples.IBM

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object PeopleInfoCalculator {
  def main(args: Array[String]) {
/*    if (args.length < 1) {
      println("Usage:PeopleInfoCalculator datafile")
      System.exit(1)
    }*/
    val conf = new SparkConf().setMaster("local").setAppName("Spark Exercise:People Info(Gender & Height) Calculator")
    val sc = new SparkContext(conf)
    val dataFile = sc.textFile("sample_people_info.txt", 5);
    val maleData = dataFile.filter(line => line.contains("M")).map(
      line => (line.split(" ")(1) + " " + line.split(" ")(2)))
    val femaleData = dataFile.filter(line => line.contains("F")).map(
      line => (line.split(" ")(1) + " " + line.split(" ")(2)))
    //for debug use
    //maleData.collect().foreach { x => println(x)}
    //femaleData.collect().foreach { x => println(x)}
    val maleHeightData = maleData.map(line => line.split(" ")(1).toInt)
    val femaleHeightData = femaleData.map(line => line.split(" ")(1).toInt)
    //for debug use
    //maleHeightData.collect().foreach { x => println(x)}
    //femaleHeightData.collect().foreach { x => println(x)}
    val lowestMale = maleHeightData.sortBy(x => x, true).first()
    val lowestFemale = femaleHeightData.sortBy(x => x, true).first()
    //for debug use
    //maleHeightData.collect().sortBy(x => x).foreach { x => println(x)}
    //femaleHeightData.collect().sortBy(x => x).foreach { x => println(x)}
    val highestMale = maleHeightData.sortBy(x => x, false).first()
    val highestFemale = femaleHeightData.sortBy(x => x, false).first()
    println("Number of Male Peole:" + maleData.count())
    println("Number of Female Peole:" + femaleData.count())
    println("Lowest Male:" + lowestMale)
    println("Lowest Female:" + lowestFemale)
    println("Highest Male:" + highestMale)
    println("Highest Female:" + highestFemale)
  }
}