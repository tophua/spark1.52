package SparkDemo

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liush on 17-7-21.
  */
object RDDcoalesceAndRepartition extends App{
  /**
    * coalesce 该函数用于将RDD进行重分区，使用HashPartitioner。
    * 第一个参数为重分区的数目，第二个为是否进行shuffle，默认为false;
    */
  val sparkConf = new SparkConf().setAppName("SparkHdfsLR").setMaster("local[2]")
  val sc = new SparkContext(sparkConf)
  var data = sc.textFile("file:/home/liush/s3/S3_2016001.txt")
  data.collect
  println("RDD data默认分区:"+data.partitions.size)
  var rdd1 = data.coalesce(1)
  //减少分区
  println("RDD coalesce(1)分区:"+rdd1.partitions.size)
  var rdd4 = data.coalesce(4)
  //coalesce 如果重分区的数目大于原来的分区数,那么必须指定shuffle参数为true,否则,分区数不变
  println("RDD coalesce(4)分区:"+rdd4.partitions.size)
  var rdd5 = data.coalesce(4,true)
  println("RDD coalesce(4,true)分区:"+rdd5.partitions.size)
  //repartition 该函数其实就是coalesce函数第二个参数为true的实现
  var rdd24 = data.repartition(1)
  println( rdd24.partitions.size)
  var rdd25 = data.repartition(4)
  println(rdd25.partitions.size)

  /**================**/
  val rdd=sc.makeRDD(1 to 10,100)
  val repartitionRDD=rdd.repartition(4)
  val coalesceRDD=rdd.coalesce(3)
  coalesceRDD.collect()
  val coalesceRDDTrue=rdd.coalesce(3,true)
  coalesceRDDTrue.collect()
  val coalesceRDDExtend=rdd.coalesce(200)
  val coalesceRDDExtenda=rdd.coalesce(200,true)
 // println(repartitionRDD.collectPartitions())

}
