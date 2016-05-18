package org.apache.spark.examples.IBM
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkWordCount2 {
  def FILE_NAME: String = "word_count_results_";
  def main(args: Array[String]) {
   /* if (args.length < 1) {
      println("Usage:SparkWordCount FileName");
      System.exit(1);
    }*/
    
    
    
    val conf = new SparkConf().setMaster("local").setAppName("Spark Exercise: Spark Version Word Count Program");
    val sc = new SparkContext(conf);
    
    val textFile = sc.textFile("people.txt");
    
    //flatMap与map类似，区别是原RDD中的元素经map处理后只能生成一个元素，而原RDD中的元素经flatmap处理后可生成多个元素来构建新RDD。
    val wordCounts = textFile.flatMap(line => line.split(" ")).map(
      word => (word, 1))
       wordCounts.foreach(e => {
    //val (k,v) = e
     println(e._1+"="+e._2)
    });
    
    /**
     * scala> val a = sc.parallelize(List((1,2),(3,4),(3,6)))
		 * scala> a.reduceByKey((x,y) => x + y).collect
		 * res7: Array[(Int, Int)] = Array((1,2), (3,10))
     */
    
     ///educeByKey就是对元素为KV对的RDD中Key相同的元素的Value进行reduce，
     //Key相同的多个元素的值被reduce为一个值，然后与原RDD中的Key组成一个新的KV对
  
    val tsets=  wordCounts.reduceByKey(_+_)//educeByKey对Key相同的元素的值求和
    //print the results,for debug use.
    //println("Word Count program running results:");
    //collect将RDD转成Scala数组，并返回
   tsets.collect().foreach(e => {
    val (k,v) = e
     println(k+"="+v)
    });
    
   wordCounts.saveAsTextFile(FILE_NAME + System.currentTimeMillis());
    println("Word Count program running results are successfully saved.");
  }
}