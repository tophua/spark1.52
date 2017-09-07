package org.apache.spark.examples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkRDDAPIExamples {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Spark Exercise: Spark Version Word Count Program");
    val sc = new SparkContext(conf);
    //检查点例子
    sc.setCheckpointDir("my_directory_name")
    val a = sc.parallelize(1 to 4)
    a.checkpoint //保存检查点,存储目录
    a.count
    //CollectAsMap例子
    val a1 = sc.parallelize(List(1, 2, 1, 3), 1)
    //zip函数将传进来的参数中相应位置上的元素组成一个pair数组。
    //如果其中一个参数元素比较长,那么多余的参数会被删掉
    val b = a1.zip(a1)
    b.collectAsMap //Map(2 -> 2, 1 -> 1, 3 -> 3)
    println(b.collectAsMap)
    //sortBy 例子
    val data = List(3,1,90,3,5,12)
    val rdd = sc.parallelize(data)
    rdd.collect
    //默认升序,Array(1, 3, 3, 5, 12, 90)
    rdd.sortBy(x => x).collect
    //使用降序,Array(90, 12, 5, 3, 3, 1)
    rdd.sortBy(x => x, false).collect.toString()
  }
}