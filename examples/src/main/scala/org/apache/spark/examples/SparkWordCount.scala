package org.apache.spark.examples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.util._

object SparkWordCount {
  def FILE_NAME: String = "word_count_results_";
  def main(args: Array[String]) {
    /* if (args.length < 1) {
      println("Usage:SparkWordCount FileName");
      System.exit(1);
    }*/

    /*    
    val creationSite: CallSite = Utils.getCallSite()

  println(">>>>>>>>>>>"+creationSite.longForm)*/

    val conf = new SparkConf().setMaster("spark://mzhy:7077").setAppName("Spark Exercise: Spark Version Word Count Program")
    //conf.setSparkHome("D:/spark/spark-1.5.0-hadoop2.6/");
    
    // .set("spark.driver.memory","512")
    // .set("spark.driver.host","192.168.10.198")
     // .set("spark.driver.port","4981")
     //.set("spark.executor.memory","2G")
     //.set("spark.cores.max", "8")
    //conf.set("SPARK_MASTER_IP", "mzhy")
    //conf.set("spark.executor.memory", "3000m")
    val sc = new SparkContext(conf);
    sc.parallelize(1 until 10000).count
    sc.parallelize(1 until 10000).map( x => (x%30,x)).groupByKey().count
    val textFile = sc.textFile("people.txt");//textFile：生成一个HadoopRDD.map,创建了一个MapPartitionsRDD
    //flatMap：前面生成的MapPartitionsRDD[String]，调用flatMap函数，生成了MapPartitionsRDD[String] 
    val wordCounts = textFile.flatMap(line => line.split(" ")).map(
      word => (word, 1))
      
    val numAs = textFile.filter(line => line.contains("a")).count()
    val numBs = textFile.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    /**
     * wordCounts.collect().foreach(e => {
     * val (k,v) = e
     * println(k+"="+v)
     * });
     *
     *
     * val wordcount = textFile.flatMap(_.split(' ')).map((_,1)).reduceByKey(_+_)
     * .map(x => (x._2, x._1))//K V调换位置
     * .sortByKey(false)//K表示倒序排列
     * .map(x => (x._2, x._1))//K V调换位置
     */
    val tsets = wordCounts.reduceByKey((a, b) => a + b) //reduceByKey(_+_)功能一样 
    //print the results,for debug use.
    //println("Word Count program running results:");

   /*tsets.collect().foreach(e => {
    val (k,v) = e
     println(k+"="+v)
    });*/
   // val sum1=tsets.collect().count()

   //wordCounts.saveAsTextFile(FILE_NAME + System.currentTimeMillis());
    println("Word Count program running results are successfully saved.");
  }
}