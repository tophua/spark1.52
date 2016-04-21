package org.apache.sparktest
import java.lang.management.ManagementFactory
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props

class Heng(ha: ActorRef) extends Actor {
  def receive = {
    case "start" => ha ! "heng"
    case "ha" =>
      println("哈")
      ha ! "heng"
    case _ => println("heng what?")
  }
}
class Ha extends Actor {
  def receive = {
    case "heng" =>
      println("哼")
      sender ! "ha"
    case _ => println("ha what?")
  }
}

object HengHa {
  def main(args: Array[String]): Unit = {
    //ActorSystem是重量级的对象，会创建1...N个线程，所以一个application一个ActorSystem
    val system = ActorSystem("HengHaSystem")
   //actorOf要创建Actor,
    val ha = system.actorOf(Props[Ha], name = "ha")
    val heng = system.actorOf(Props(new Heng(ha)), name = "heng")

    //heng ! "start"    
    
    //ManagementFactory.getGarbageCollectorMXBeans.map(_.getCollectionTime).sum
  }
}