package sparkDemo

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Created by liush on 17-7-13.
  */
object PartitionByDemo {
  def main(args: Array[String]):Unit= {
    val conf = new SparkConf().setMaster("local").setAppName("testPartitionBy")
    val sc = new SparkContext(conf)
    //val logFileA = sc.textFile("C:/Users/lh/Desktop/10G/tpch.log").map((_,1))
    val logFileA = sc.textFile("/home/liush/s3/S3_2016001.txt",1).map((_,1))
    println( "Before new partition:"+logFileA.partitions.size )
    val logDataB=logFileA.partitionBy(new HashPartitioner(2))
    println( "After new partition:"+logDataB.partitions.size )
    logDataB.partitions.foreach { partition =>
      println("index:" + partition.index + "  hasCode:" + partition.hashCode())
    }
    println(sc.parallelize(Seq(1, 2, 3), 3).filter(_ < 0).isEmpty())

    logDataB.filter(_._1.startsWith("94266375d0a8")).foreach(println)
    logDataB.filter{case (k, v) => k.contains("94266375d0a8")}.foreach(println)
    logFileA.foreach(println _)

    //logFileA.filter()


   /* def startsWith(prefix: String, toffset: Int) = {

      true
    }*/

    /* logFileA.filter((a:String,b:Int)=>{
      a.startsWith("942663")
     })  */


/*   logDataB.filter(a =>{
      System.out.println(a)
      //a.startWith("ERROR")
    })
    val logFileD=logFileC.filter(_=>_.length>30)
    logFileC.cache().count();
    logFileD.count();*/
  }
}
