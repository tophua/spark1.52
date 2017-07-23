package scalaDemo

import java.text.SimpleDateFormat
import java.util.{Timer, TimerTask}

object TimerSchedule {
  /**
    * 在java中,Timer类主要用于定时性、周期性任务 的触发,这个类中有两个方法比较难理解,
    * 那就是schedule和scheduleAtFixedRate方法,在这里就用实例分析一下
    * （1）schedule方法：“fixed-delay”；如果第一次执行时间被delay了,随后的执行时间按 照 上一次 实际执行完成的时间点 进行计算
    * （2）scheduleAtFixedRate方法：“fixed-rate”；如果第一次执行时间被delay了,随后的执行时间按照 上一次开始的 时间点 进行计算,
    * 并且为了”catch up”会多次执行任务,TimerTask中的执行体需要考虑同步
    *
    *
    */
  def main(args: Array[String]) {

    /**
      *
      * 以上的代码,表示在2010-11-26 00:20:00秒开始执行,每3分钟执行一次
      * 假设在2010/11/26 00:27:00执行
      * 以上会打印出3次
      * execute task!   00:20
      * execute task!   00:23    catch up
      * execute task!   00:26    catch up
      * 下一次执行时间是00:29,相对于00:26
      * 当换成schedule方法时,在2010/11/26 00:27:00执行
      * 会打印出1次
      * execute task!   00:20   无catch up
      * 下一次执行时间为00:30,相对于00:27
      * 以上考虑的都是在你设定的timer开始时间后,程序才被执行
      */
    val fTime = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    val d1 = fTime.parse("2005/12/30 14:10:00");
    val timer: Timer = new Timer();
    timer.scheduleAtFixedRate(new TimerTask() {
      override def run(): Unit = {
        System.out.println("this is task you do6");
      }
    }, d1, 3 * 60 * 1000);

  }
}