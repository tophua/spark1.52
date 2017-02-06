package org.apache.spark.examples.IBM
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.HashPartitioner
import org.apache.spark.streaming.Duration
/**
 * 具体参考:
 * https://www.ibm.com/developerworks/cn/opensource/os-cn-spark-practice2/
 */
object WebPagePopularityValueCalculator {

  private val checkpointDir = "popularity-data-checkpoint"
  private val msgConsumerGroup = "user-behavior-topic-message-consumer-group"

  def main(args: Array[String]) {
    /*    if (args.length < 2) {
      println("Usage:WebPagePopularityValueCalculator zkserver1:2181,zkserver2:2181,zkserver3:2181 consumeMsgDataTimeInterval(secs)")
      System.exit(1)
    }*/
    val Array(zkServers, processingInterval) = Array("192.168.0.39:2181", "2")
    val jarpath = "D:\\eclipse44_64\\workspace\\spark1.5\\examples\\lib\\"
    val conf = new SparkConf().setAppName("Web Page Popularity Value Calculator").setMaster("spark://dept3:8088")
      /** val conf = new SparkConf().setMaster("spark://dept3:8088").setAppName("Chapter01")**/
      .set("spark.driver.port", "8088")
      .set("spark.fileserver.port", "3306")
      .set("spark.replClassServer.port", "8080")
      .set("spark.broadcast.port", "8089")
      .set("spark.blockManager.port", "15000")
      .setJars(Array(jarpath + "spark-streaming-kafka_2.10-1.5.3-20151212.002413-63.jar", jarpath + "kafka_2.10-0.10.1.0.jar", jarpath + "kafka-clients-0.10.1.0.jar", jarpath + "zkclient-0.3.jar", jarpath + "metrics-core-2.2.0.jar", "D:\\testjar\\spark-examples-ibm.jar"))
    val ssc = new StreamingContext(conf, Seconds(processingInterval.toInt))
    //using updateStateByKey asks for enabling checkpoint
    //并且开启 checkpoint 功能,因为我们需要使用 updateStateByKey 原语去累计的更新网页话题的热度值
    ssc.checkpoint(checkpointDir)
    //利用 Spark提供的 KafkaUtils.createStream方法消费消息主题,这个方法会返回 ReceiverInputDStream对象实例
    val kafkaStream = KafkaUtils.createStream(
      //Spark streaming context
      //创建一个StreamingContext对象
      ssc,
      //zookeeper quorum. e.g zkserver1:2181,zkserver2:2181,...zookeeper服务器位置
      //配置Zookeeper的信息,因为要高可用,所以用Zookeeper进行集群监控、自动故障恢复
      zkServers,
      //kafka message consumer group ID
      //kafka消息消费组
      msgConsumerGroup,
      //Map of (topic_name -> numPartitions) to consume. Each partition is consumed in its own thread
      //Map of (topic_name -> numPartitions) to 消费者,每个分区都在自己的线程中被消耗掉
      Map("user-behavior-topic" -> 3))
    //
    val msgDataRDD = kafkaStream.map(_._2)
    //for debug use only
    //仅用于调试
    //在此间隔中的数据
    println("Coming data in this interval...")
    //msgDataRDD.print()
    // e.g page37|5|1.5119122|-1
    val popularityData = msgDataRDD.map { msgLine =>
      {
        val dataArr: Array[String] = msgLine.split("\\|")
        val pageID = dataArr(0)
        //calculate the popularity value
        //对于每一条消息,利用上文的公式计算网页话题的热度值
        val popValue: Double = dataArr(1).toFloat * 0.8 + dataArr(2).toFloat * 0.8 + dataArr(3).toFloat * 1
        (pageID, popValue)
      }
    }
    //sum the previous popularity value and current value
    //定义一个匿名函数去把网页热度上一次的计算结果值和新计算的值相加,得到最新的热度值
    val updatePopularityValue = (iterator: Iterator[(String, Seq[Double], Option[Double])]) => {
      iterator.flatMap(t => {
        val newValue: Double = t._2.sum
        val stateValue: Double = t._3.getOrElse(0);
        Some(newValue + stateValue)
      }.map(sumedValue => (t._1, sumedValue)))
    }
    //
    val initialRDD = ssc.sparkContext.parallelize(List(("page1", 0.00)))
    //调用 updateStateByKey 原语并传入上面定义的匿名函数更新网页热度值
    //
    val stateDstream = popularityData.updateStateByKey[Double](updatePopularityValue,
      new HashPartitioner(ssc.sparkContext.defaultParallelism), true, initialRDD)
    //set the checkpoint interval to avoid too frequently data checkpoint which may
    //may significantly reduce operation throughput
    //设置检查点间隔,以避免过于频繁的数据检查点,这可能会显着降低操作吞吐量
    stateDstream.checkpoint(Duration(8 * processingInterval.toInt * 1000))
    //after calculation, we need to sort the result and only show the top 10 hot pages
    //计算后,我们需要对结果进行排序,只显示前10个热门页面
    stateDstream.foreachRDD { rdd =>
      {
        val sortedData = rdd.map { case (k, v) => (v, k) }.sortByKey(false)
        val topKData = sortedData.take(10).map { case (v, k) => (k, v) }
        topKData.foreach(x => {
          println(x)
        })
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}