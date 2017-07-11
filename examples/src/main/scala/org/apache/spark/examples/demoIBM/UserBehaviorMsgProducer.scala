package org.apache.spark.examples.demoIBM
import scala.util.Random
import java.util.Properties
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig
import kafka.producer.Producer
/**
 * https://www.ibm.com/developerworks/cn/opensource/os-cn-spark-practice2/
 * 生产行为数据消息
 * 该程序每隔 5 秒钟会随机的向 user-behavior-topic 主题推送 0 到 50 条行为数据消息,
 * 这个程序扮演消息生产者的角色,在实际应用中,这个功能一般会由一个系统来提供.
 * 为了简化消息处理,我们定义消息的格式如下:网页 ID|点击次数|停留时间 (分钟)|是否点赞
 */
class UserBehaviorMsgProducer(brokers: String, topic: String) extends Runnable {
  private val brokerList = brokers
  private val targetTopic = topic
  private val props = new Properties()
  props.put("metadata.broker.list", this.brokerList)
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("producer.type", "async")
  private val config = new ProducerConfig(this.props)
  private val producer = new Producer[String, String](this.config)

  private val PAGE_NUM = 100
  private val MAX_MSG_NUM = 3
  private val MAX_CLICK_TIME = 5
  private val MAX_STAY_TIME = 10
  //代表是否点赞,1 为赞,-1 表示踩,0 表示中立
  //Like,1;Dislike -1;No Feeling 0
  private val LIKE_OR_NOT = Array[Int](1, 0, -1)

  def run(): Unit = {
    val rand = new Random()
    while (true) {
      //how many user behavior messages will be produced
      //将产生多少用户行为消息
      val msgNum = rand.nextInt(MAX_MSG_NUM) + 1
      try {
        //generate the message with format like page1|2|7.123|1
        //产生消息的格式像 
        for (i <- 0 to msgNum) {
          var msg = new StringBuilder()
          msg.append("page" + (rand.nextInt(PAGE_NUM) + 1))
          msg.append("|")
          msg.append(rand.nextInt(MAX_CLICK_TIME) + 1)
          msg.append("|")
          msg.append(rand.nextInt(MAX_CLICK_TIME) + rand.nextFloat())
          msg.append("|")
          msg.append(LIKE_OR_NOT(rand.nextInt(3)))
          println(msg.toString())
          //send the generated message to broker
          sendMessage(msg.toString())
        }
        println("%d user behavior messages produced.".format(msgNum + 1))
      } catch {
        case e: Exception => println(e)
      }
      try {
        //sleep for 5 seconds after send a micro batch of message
        Thread.sleep(5000)
      } catch {
        case e: Exception => println(e)
      }
    }
  }
  def sendMessage(message: String) = {
    try {
      val data = new KeyedMessage[String, String](this.topic, message);
      producer.send(data);
    } catch {
      case e: Exception => println(e)
    }
  }
}
object UserBehaviorMsgProducerClient {
  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Usage:UserBehaviorMsgProducerClient 192.168.1.1:9092 user-behavior-topic")
      System.exit(1)
    }
    //start the message producer thread
    new Thread(new UserBehaviorMsgProducer(args(0), args(1))).start()
  }
}
