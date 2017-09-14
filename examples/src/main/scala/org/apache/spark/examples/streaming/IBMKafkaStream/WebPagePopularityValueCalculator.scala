package org.apache.spark.examples.streaming.IBMKafkaStream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.HashPartitioner
import org.apache.spark.streaming.Duration

object WebPagePopularityValueCalculator {
  private val checkpointDir = "popularity-data-checkpoint"
  private val msgConsumerGroup = "user-behavior-topic-message-consumer-group"

  def main(args: Array[String]) {
  /*  if (args.length < 2) {
      println("Usage:WebPagePopularityValueCalculator zkserver1:2181, zkserver2:2181,zkserver3:2181 consumeMsgDataTimeInterval(secs)")
      System.exit(1)
    }*/
    val Array(zkServers,processingInterval) = Array("store-node5:2181,store-node6:2181,store-node8:2181","2")//args
    val conf = new SparkConf().setAppName("Web Page Popularity Value Calculator").setMaster("local")
    val ssc = new StreamingContext(conf, Seconds(processingInterval.toInt))
    //using updateStateByKey asks for enabling checkpoint
    //并且开启 checkpoint 功能,因为我们需要使用 updateStateByKey 原语去累计的更新网页话题的热度值
    ssc.checkpoint(checkpointDir)
    // KafkaUtils.createStream 方法消费消息主题，这个方法会返回 ReceiverInputDStream 对象实例
    val kafkaStream = KafkaUtils.createStream(
      //Spark streaming context
      ssc,
      //zookeeper quorum. e.g zkserver1:2181,zkserver2:2181,...
      zkServers,
      //kafka message consumer group ID
      msgConsumerGroup,
      //Map of (topic_name -> numPartitions) to consume. Each partition is consumed in its own thread
      Map("topic" -> 2))
    val msgDataRDD = kafkaStream.map(_._2)
    //for debug use only
    //println("Coming data in this interval...")
    //msgDataRDD.print()
    // e.g page37|5|1.5119122|-1
    //网页 ID|点击次数|停留时间 (分钟)|是否点赞
    //向量的第一项表示网页的ID,第二项表示从进入网站到离开对该网页的点击次数,第三项表示停留时间,以分钟为单位,第四项是代表是否点赞,1 为赞,-1 表示踩,0 表示中立
    //对于每一条消息，利用上文的公式计算网页话题的热度值。
    val popularityData = msgDataRDD.map { msgLine =>
    {
      val dataArr: Array[String] = msgLine.split("\\|")
      val pageID = dataArr(0)
      //calculate the popularity value
      //点击次数权重是 0.8
      //停留时间权重是 0.8
      //是否点赞权重是 1，因为这一般表示用户对该网页的话题很有兴趣
      //f(x,y,z)=0.8x+0.8y+z
     // 那么对于上面的行为数据 (page001.html, 1, 0.5, 1)，利用公式可得：
      //H(page001)=f(x,y,z)= 0.8x+0.8y+z=0.8*1+0.8*0.5+1*1=2.2
      val popValue: Double = dataArr(1).toFloat * 0.8 + dataArr(2).toFloat * 0.8 + dataArr(3).toFloat * 1
      (pageID, popValue)
    }
    }
    //sum the previous popularity value and current value
    //定义一个匿名函数去把网页热度上一次的计算结果值和新计算的值相加,得到最新的热度值
    //入参是三元组遍历器,
    /**
      * (Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)]如何解读？
        入参： 三元组迭代器，三元组中K表示Key,Seq[V]表示一个时间间隔中产生的Key对应的Value集合(Seq类型,需要对这个集合定义累加函数逻辑进行累加),
              Option[S]表示上个时间间隔的累加值(表示这个Key上个时间点的状态)
        出参：二元组迭代器，二元组中K表示Key，S表示当前时间点执行结束后，得到的累加值(即最新状态)
      */
    val updatePopularityValue = (iterator: Iterator[(String, Seq[Double], Option[Double])]) => {
      iterator.flatMap(t => {
        //page37|5|1.5119122|-1
        //通过Spark内部的reduceByKey按key规约,然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
        val newValue:Double = t._2.sum
        //已累加的值
        val stateValue:Double = t._3.getOrElse(0)
        /**
          * ==t1==page45==t2==CompactBuffer()==t3==Some(12.329806232452393)
          *==t1==page53==t2==CompactBuffer()==t3==Some(16.665557193756104)
          *==t1==page18==t2==CompactBuffer()==t3==Some(10.105697631835938)
          *==t1==page28==t2==CompactBuffer()==t3==Some(5.347046375274658)
          *
          *==t1==page89==t2==5.00500717163086==t3==Some(19.28474450111389)
          */
        println("==t1=="+ t._1+"==t2=="+ t._2.mkString(",")+"==t3=="+ t._3)
        // 返回累加后的结果,是一个Option[Int]类型
        Some(newValue + stateValue)
        //page45,12.329806232452393
      }.map(sumedValue => (t._1, sumedValue)))
    }
    /**
      * initialRDD是（K，S）类型的RDD，它表示一组Key的初始状态，每个（K，S）表示一个Key以及它对应的State状态。
      * K表示updateStateByKey的Key的类型，比如String，而S表示Key对应的状态（State）类型，在上例中，是Int
      */
    val initialRDD = ssc.sparkContext.parallelize(List(("page1", 0.00)))
    //调用 updateStateByKey 原语并传入上面定义的匿名函数更新网页热度值。
    //rememberPartitioner：true 表示是否在接下来的Spark Streaming执行过程中产生的RDD使用相同的分区算法
    //updatePopularityValue是函数常量,类型为(Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)]，表示状态更新函数
    /**
      * updateStateByKey 解释:
        以DStream中的数据进行按key做reduce操作,然后对各个批次的数据进行累加
        在有新的数据信息进入或更新时,可以让用户保持想要的任何状。使用这个功能需要完成两步：
        1) 定义状态：可以是任意数据类型
        2) 定义状态更新函数：用一个函数指定如何使用先前的状态,从输入流中的新值更新状态。
        对于有状态操作，要不断的把当前和历史的时间切片的RDD累加计算，随着时间的流失，计算的数据规模会变得越来越大。
      */
    val stateDstream = popularityData.updateStateByKey[Double](updatePopularityValue,
      new HashPartitioner(ssc.sparkContext.defaultParallelism), true, initialRDD)
    //set the checkpoint interval to avoid too frequently data checkpoint which may
    //may significantly reduce operation throughput
    //设置检查点间隔以避免太频繁的数据检查点,这可能会显着降低操作吞吐量
    stateDstream.checkpoint(Duration(8*processingInterval.toInt*1000))
    //after calculation, we need to sort the result and only show the top 10 hot pages
    //最后得到最新结果后，需要对结果进行排序，最后打印热度值最高的 10 个网页。
    stateDstream.foreachRDD { rdd => {
      val sortedData = rdd.map{ case (k,v) => (v,k) }.sortByKey(false)
      val topKData = sortedData.take(10).map{ case (v,k) => (k,v) }
      topKData.foreach(x => {
        println("========"+x)
      })

    }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}