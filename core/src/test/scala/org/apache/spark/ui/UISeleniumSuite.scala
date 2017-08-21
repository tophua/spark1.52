/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ui

import java.net.{HttpURLConnection, URL}
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}

import scala.collection.JavaConversions._
import scala.xml.Node

import com.gargoylesoftware.htmlunit.DefaultCssErrorHandler
import org.json4s._
import org.json4s.jackson.JsonMethods
import org.openqa.selenium.htmlunit.HtmlUnitDriver
import org.openqa.selenium.{By, WebDriver}
import org.scalatest._
import org.scalatest.concurrent.Eventually._
import org.scalatest.selenium.WebBrowser
import org.scalatest.time.SpanSugar._
import org.w3c.css.sac.CSSParseException

import org.apache.spark.LocalSparkContext._
import org.apache.spark._
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.deploy.history.HistoryServerSuite
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.status.api.v1.{JacksonMessageWriter, StageStatus}

private[spark] class SparkUICssErrorHandler extends DefaultCssErrorHandler {

  private val cssWhiteList = List("bootstrap.min.css", "vis.min.css")

  private def isInWhileList(uri: String): Boolean = cssWhiteList.exists(uri.endsWith)

  override def warning(e: CSSParseException): Unit = {
    if (!isInWhileList(e.getURI)) {
      super.warning(e)
    }
  }

  override def fatalError(e: CSSParseException): Unit = {
    if (!isInWhileList(e.getURI)) {
      super.fatalError(e)
    }
  }

  override def error(e: CSSParseException): Unit = {
    if (!isInWhileList(e.getURI)) {
      super.error(e)
    }
  }
}

/**
 * Selenium tests for the Spark Web UI.
  * Selenium测试Spark Web UI
  * Selenium测试工具
 */
class UISeleniumSuite extends SparkFunSuite with WebBrowser with Matchers with BeforeAndAfterAll {

  implicit var webDriver: WebDriver = _
  implicit val formats = DefaultFormats


  override def beforeAll(): Unit = {
    webDriver = new HtmlUnitDriver {
      getWebClient.setCssErrorHandler(new SparkUICssErrorHandler)
    }
  }

  override def afterAll(): Unit = {
    if (webDriver != null) {
      webDriver.quit()
    }
  }

  /**
   * Create a test SparkContext with the SparkUI enabled.
   * It is safe to `get` the SparkUI directly from the SparkContext returned here.
    * 在启用SparkUI的情况下创建一个测试SparkContext,从这里返回的SparkContext直接“获取”SparkUI是安全的。
   */
  private def newSparkContext(killEnabled: Boolean = true): SparkContext = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set("spark.ui.enabled", "true")
      .set("spark.ui.port", "0")
      //允许在webUI将stage和相应的job杀死
      .set("spark.ui.killEnabled", killEnabled.toString)
    val sc = new SparkContext(conf)
    assert(sc.ui.isDefined)
    sc
  }
  //应该体现unpersist()/ persist()的效果
  test("effects of unpersist() / persist() should be reflected") {
    // Regression test for SPARK-2527
    //
    withSpark(newSparkContext()) { sc =>
      val ui = sc.ui.get
      val rdd = sc.parallelize(Seq(1, 2, 3))
      rdd.persist(StorageLevels.DISK_ONLY).count()
      eventually(timeout(5 seconds), interval(50 milliseconds)) {
        goToUi(ui, "/storage")
        //ParallelCollectionRDD Disk Serialized 1x Replicated 1 100% 0.0 B 0.0 B 101.0 B
        val tableRowText = findAll(cssSelector("#storage-by-rdd-table td")).map(_.text).toSeq
        println(tableRowText.mkString(" "))
        //Disk Serialized 1x Replicated
        println(StorageLevels.DISK_ONLY.description)
        //tableRowText.foreach(println _)
        tableRowText should contain (StorageLevels.DISK_ONLY.description)
      }
      eventually(timeout(5 seconds), interval(50 milliseconds)) {
        goToUi(ui, "/storage/rdd/?id=0")
        //rdd_0_0 Disk Serialized 1x Replicated 0.0 B 101.0 B localhost:39446
        val tableRowText = findAll(cssSelector("#rdd-storage-by-block-table td")).map(_.text).toSeq
        println(tableRowText.mkString(" "))
        //Disk Serialized 1x Replicated
        tableRowText should contain (StorageLevels.DISK_ONLY.description)
      }

      val storageJson = getJson(ui, "storage/rdd")
      println(storageJson.toOption.mkString(" "))
      storageJson.children.length should be (1)
      (storageJson \ "storageLevel").extract[String] should be (StorageLevels.DISK_ONLY.description)
      val rddJson = getJson(ui, "storage/rdd/0")
      (rddJson  \ "storageLevel").extract[String] should be (StorageLevels.DISK_ONLY.description)

      rdd.unpersist()
      rdd.persist(StorageLevels.MEMORY_ONLY).count()
      eventually(timeout(5 seconds), interval(50 milliseconds)) {
        goToUi(ui, "/storage")
        val tableRowText = findAll(cssSelector("#storage-by-rdd-table td")).map(_.text).toSeq
        println(tableRowText.mkString(" "))
        tableRowText should contain (StorageLevels.MEMORY_ONLY.description)
      }
      eventually(timeout(5 seconds), interval(50 milliseconds)) {
        goToUi(ui, "/storage/rdd/?id=0")
        val tableRowText = findAll(cssSelector("#rdd-storage-by-block-table td")).map(_.text).toSeq
        println(tableRowText.mkString(" "))
        tableRowText should contain (StorageLevels.MEMORY_ONLY.description)
      }

      val updatedStorageJson = getJson(ui, "storage/rdd")
      println(updatedStorageJson.toOption.mkString(" "))
      updatedStorageJson.children.length should be (1)

      (updatedStorageJson \ "storageLevel").extract[String] should be (
        StorageLevels.MEMORY_ONLY.description)
      val updatedRddJson = getJson(ui, "storage/rdd/0")
      println(updatedRddJson.toOption.mkString(" "))
      (updatedRddJson  \ "storageLevel").extract[String] should be (
        StorageLevels.MEMORY_ONLY.description)
    }
  }
  //失败的阶段不应该似乎是活跃的
  test("failed stages should not appear to be active") {//失败的阶段不应该出现是活跃的
    withSpark(newSparkContext()) { sc =>
      // Regression test for SPARK-3021
      intercept[SparkException] {
        sc.parallelize(1 to 10).map { x => throw new Exception()}.collect()
      }
      eventually(timeout(5 seconds), interval(50 milliseconds)) {
        goToUi(sc, "/stages")
        //因为我们隐藏空表
        find(id("active")) should be(None)  // Since we hide empty tables
        find(id("failed")).get.text should be("Failed Stages (1)")
      }
      val stageJson = getJson(sc.ui.get, "stages")
      stageJson.children.length should be (1)
      (stageJson \ "status").extract[String] should be (StageStatus.FAILED.name())

      // Regression test for SPARK-2105
      class NotSerializable
      val unserializableObject = new NotSerializable
      intercept[SparkException] {
        sc.parallelize(1 to 10).map { x => unserializableObject}.collect()
      }
      eventually(timeout(5 seconds), interval(50 milliseconds)) {
        goToUi(sc, "/stages")
        //因为我们隐藏空表
        find(id("active")) should be(None)  // Since we hide empty tables
        // The failure occurs before the stage becomes active, hence we should still show only one
        // failed stage, not two:
        //失败发生在阶段活动之前，因此我们仍然只显示一个失败的阶段,而不是两个
        find(id("failed")).get.text should be("Failed Stages (1)")
      }
      //===List(Map(name -> collect at UISeleniumSuite.scala:175, schedulingPool -> default, stageId -> 0,
      // outputBytes -> 0, outputRecords -> 0, diskBytesSpilled -> 0, numCompleteTasks -> 0, shuffleWriteBytes -> 0,
      // shuffleReadBytes -> 0, attemptId -> 0, numActiveTasks -> 0, status -> FAILED, inputBytes -> 0,
      // memoryBytesSpilled -> 0, inputRecords -> 0, details -> org.apache.spark.rdd.RDD.collect(RDD.scala:1056)
      val updatedStageJson = getJson(sc.ui.get, "stages")
      println("==="+updatedStageJson.values)
      updatedStageJson should be (stageJson)
    }
  }
  //允许在webUI将stage和相应的job杀死
  test("spark.ui.killEnabled should properly control kill button display") {
    //调用时计算
    def hasKillLink: Boolean = find(className("kill-link")).isDefined
    //false
    println(hasKillLink)
    def runSlowJob(sc: SparkContext) {
      sc.parallelize(1 to 10).map{x => Thread.sleep(10000); x}.countAsync()
    }

    withSpark(newSparkContext(killEnabled = true)) { sc =>
      runSlowJob(sc)
      eventually(timeout(5 seconds), interval(50 milliseconds)) {
        goToUi(sc, "/stages")
        //true 调用时计算hasKillLink
        println(hasKillLink)
        assert(hasKillLink)
      }
    }

    withSpark(newSparkContext(killEnabled = false)) { sc =>
      runSlowJob(sc)
      eventually(timeout(5 seconds), interval(50 milliseconds)) {
        goToUi(sc, "/stages")
        assert(!hasKillLink)
      }
    }
  }
  //作业页面不应显示作业组名称,除非在作业组中提交了一些作业
  test("jobs page should not display job group name unless some job was submitted in a job group") {
    withSpark(newSparkContext()) { sc =>
      // If no job has been run in a job group, then "(Job Group)" should not appear in the header
      //如果作业组中没有运行作业，则“（作业组）”不应出现在标题中
      sc.parallelize(Seq(1, 2, 3)).count()
      eventually(timeout(5 seconds), interval(50 milliseconds)) {
        goToUi(sc, "/jobs")
        val tableHeaders = findAll(cssSelector("th")).map(_.text).toSeq
        //Job Id Description Submitted Duration Stages: Succeeded/Total Tasks (for all stages): Succeeded/Total
        println(tableHeaders.mkString(" "))
        tableHeaders should not contain "Job Id (Job Group)"
      }
      // Once at least one job has been run in a job group, then we should display the group name:
      //至少有一个作业在作业组中运行，那么我们应该显示组名：
      sc.setJobGroup("my-job-group", "my-job-group-description")
      sc.parallelize(Seq(1, 2, 3)).count()
      eventually(timeout(5 seconds), interval(50 milliseconds)) {
        goToUi(sc, "/jobs")
        val tableHeaders = findAll(cssSelector("th")).map(_.text).toSeq
        //Job Id (Job Group) Description Submitted Duration Stages: Succeeded/Total Tasks (for all stages): Succeeded/Total
        println(tableHeaders.mkString(" "))
        tableHeaders should contain ("Job Id (Job Group)")
      }

      val jobJson = getJson(sc.ui.get, "jobs")
      for {
        job @ JObject(_) <- jobJson
        JInt(jobId) <- job \ "jobId"
        jobGroup = job \ "jobGroup"
      } {
        jobId.toInt match {
          case 0 => jobGroup should be (JNothing)
          case 1 => jobGroup should be (JString("my-job-group"))
        }
      }
    }
  }
  //工作进度条应处理阶段/任务失败
  test("job progress bars should handle stage / task failures") {
    withSpark(newSparkContext()) { sc =>
      val data = sc.parallelize(Seq(1, 2, 3), 1).map(identity).groupBy(identity)
      //
      val shuffleHandle =
        data.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]].shuffleHandle
      // Simulate fetch failures:
      //模拟提取故障：
      val mappedData = data.map { x =>
        //获得TaskContext
        val taskContext = TaskContext.get
        if (taskContext.taskAttemptId() == 1) {
          // Cause the post-shuffle stage to fail on its first attempt with a single task failure
          //导致洗牌阶段在第一次尝试中失败,并导致单个任务失败
          val env = SparkEnv.get
          val bmAddress = env.blockManager.blockManagerId

          val shuffleId = shuffleHandle.shuffleId
          val mapId = 0
          val reduceId = taskContext.partitionId()
          val message = "Simulated fetch failure"
          //mapId对应RDD的partionsID
          throw new FetchFailedException(bmAddress, shuffleId, mapId, reduceId, message)
        } else {
          x
        }
      }
      mappedData.count()
      eventually(timeout(5 seconds), interval(50 milliseconds)) {
        goToUi(sc, "/jobs")
        find(cssSelector(".stage-progress-cell")).get.text should be ("2/2 (1 failed)")
        // Ideally, the following test would pass, but currently we overcount completed tasks
        // if task recomputations occur:
        //理想情况下，以下测试将通过，但是目前我们已经完成了任务,如果任务重新计算发生：
        // find(cssSelector(".progress-cell .progress")).get.text should be ("2/2 (1 failed)")
        // Instead, we guarantee that the total number of tasks is always correct, while the number
        // of completed tasks may be higher:
        find(cssSelector(".progress-cell .progress")).get.text should be ("3/2 (1 failed)")
      }
      val jobJson = getJson(sc.ui.get, "jobs")
      (jobJson \ "numTasks").extract[Int]should be (2)
      (jobJson \ "numCompletedTasks").extract[Int] should be (3)
      (jobJson \ "numFailedTasks").extract[Int] should be (1)
      (jobJson \ "numCompletedStages").extract[Int] should be (2)
      (jobJson \ "numFailedStages").extract[Int] should be (1)
      val stageJson = getJson(sc.ui.get, "stages")

      for {
        stage @ JObject(_) <- stageJson
        JString(status) <- stage \ "status"
        JInt(stageId) <- stage \ "stageId"
        JInt(attemptId) <- stage \ "attemptId"
      } {
        val exp = if (attemptId == 0 && stageId == 1) StageStatus.FAILED else StageStatus.COMPLETE
        status should be (exp.name())
      }

      for {
        stageId <- 0 to 1
        attemptId <- 0 to 1
      } {
        val exp = if (attemptId == 0 && stageId == 1) StageStatus.FAILED else StageStatus.COMPLETE
        val stageJson = getJson(sc.ui.get, s"stages/$stageId/$attemptId")
        (stageJson \ "status").extract[String] should be (exp.name())
      }
    }
  }
  //作业详细信息页面应显示尚未启动的阶段的有用信息
  test("job details page should display useful information for stages that haven't started") {
    withSpark(newSparkContext()) { sc =>
      // Create a multi-stage job with a long delay in the first stage:
      //在第一阶段创建一个长时间延迟的多阶段工作：
      val rdd = sc.parallelize(Seq(1, 2, 3)).map { x =>
        // This long sleep call won't slow down the tests because we don't actually need to wait
        // for the job to finish.
        //这个长时间的睡眠呼叫不会减慢测试速度，因为我们实际上不需要等待为了完成工作
        Thread.sleep(20000)
      }.groupBy(identity).map(identity).groupBy(identity).map(identity)
      // Start the job:
      rdd.countAsync()
      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        goToUi(sc, "/jobs/job/?id=0")
        find(id("active")).get.text should be ("Active Stages (1)")
        find(id("pending")).get.text should be ("Pending Stages (2)")
        // Essentially, we want to check that none of the stage rows show
        //本质上,我们要检查没有一个stage行显示
        // "No data available for this stage". Checking for the absence of that string is brittle
        // because someone could change the error message and cause this test to pass by accident.
        // Instead, it's safer to check that each row contains a link to a stage details page.
        //“这个阶段没有数据可用”。 检查该字符串的缺失是否脆弱因为有人可以更改错误信息，导致此测试偶然发生。
        //相反，检查每行包含指向舞台细节页面的链接是更安全的。
        findAll(cssSelector("tbody tr")).foreach { row =>
          val link = row.underlying.findElement(By.xpath("./td/div/a"))
          link.getAttribute("href") should include ("stage")
        }
      }
    }
  }
  //工作进度条/单元反映了跳过的阶段/任务
  test("job progress bars / cells reflect skipped stages / tasks") {
    withSpark(newSparkContext()) { sc =>
      // Create an RDD that involves multiple stages:
      val rdd = sc.parallelize(1 to 8, 8)
        .map(x => x).groupBy((x: Int) => x, numPartitions = 8)
        .flatMap(x => x._2).groupBy((x: Int) => x, numPartitions = 8)
      // Run it twice; this will cause the second job to have two "phantom" stages that were
      // mentioned in its job start event but which were never actually executed:
      //运行两次；这将导致第二个作业有两个“幻象”阶段。在工作开始事件中提到但实际上从未执行过
      rdd.count()
      rdd.count()
      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        goToUi(sc, "/jobs")
        // The completed jobs table should have two rows. The first row will be the most recent job:
        //完成的工作表应该有两行。第一排是最近的一次工作：
        val firstRow = find(cssSelector("tbody tr")).get.underlying
        val firstRowColumns = firstRow.findElements(By.tagName("td"))
        firstRowColumns(0).getText should be ("1")
        firstRowColumns(4).getText should be ("1/1 (2 skipped)")
        firstRowColumns(5).getText should be ("8/8 (16 skipped)")
        // The second row is the first run of the job, where nothing was skipped:
        //第二行是作业的第一次运行，没有跳过任何操作：
        val secondRow = findAll(cssSelector("tbody tr")).toSeq(1).underlying
        val secondRowColumns = secondRow.findElements(By.tagName("td"))
        secondRowColumns(0).getText should be ("0")
        secondRowColumns(4).getText should be ("3/3")
        secondRowColumns(5).getText should be ("24/24")
      }
    }
  }
  //未运行的阶段在工作完成后出现“跳过阶段”
  test("stages that aren't run appear as 'skipped stages' after a job finishes") {
    withSpark(newSparkContext()) { sc =>
      // Create an RDD that involves multiple stages:
      //创建一个RDD，涉及多个阶段：
      val rdd =
        sc.parallelize(Seq(1, 2, 3)).map(identity).groupBy(identity).map(identity).groupBy(identity)
      // Run it twice; this will cause the second job to have two "phantom" stages that were
      // mentioned in its job start event but which were never actually executed:
      //运行两次；这将导致第二个作业有两个“幻象”阶段。在工作开始事件中提到但实际上从未执行过：
      rdd.count()
      rdd.count()
      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        goToUi(sc, "/jobs/job/?id=1")
        find(id("pending")) should be (None)
        find(id("active")) should be (None)
        find(id("failed")) should be (None)
        find(id("completed")).get.text should be ("Completed Stages (1)")
        find(id("skipped")).get.text should be ("Skipped Stages (2)")
        // Essentially, we want to check that none of the stage rows show
        // "No data available for this stage". Checking for the absence of that string is brittle
        // because someone could change the error message and cause this test to pass by accident.
        // Instead, it's safer to check that each row contains a link to a stage details page.
        //本质上，我们希望检查没有一个行显示“此阶段没有可用数据”。检查那根绳子的缺失是否易碎。
        //因为某人可以更改错误消息并导致此测试意外通过。
        // 相反，检查每一行包含一个到阶段详细信息页的链接是比较安全的。
        findAll(cssSelector("tbody tr")).foreach { row =>
          val link = row.underlying.findElement(By.xpath(".//a"))
          link.getAttribute("href") should include ("stage")
        }
      }
    }
  }
  //跳过阶段的作业应该在所有作业页面上显示正确的链接描述。
  test("jobs with stages that are skipped should show correct link descriptions on all jobs page") {
    withSpark(newSparkContext()) { sc =>
      // Create an RDD that involves multiple stages:
      //创建一个RDD，涉及多个阶段：
      val rdd =
        sc.parallelize(Seq(1, 2, 3)).map(identity).groupBy(identity).map(identity).groupBy(identity)
      // Run it twice; this will cause the second job to have two "phantom" stages that were
      // mentioned in its job start event but which were never actually executed:
      //运行两次；这将导致第二个作业有两个“幻象”阶段。在工作开始事件中提到但实际上从未执行过：
      rdd.count()
      rdd.count()
      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        goToUi(sc, "/jobs")
        findAll(cssSelector("tbody tr a")).foreach { link =>
          link.text.toLowerCase should include ("count")
          link.text.toLowerCase should not include "unknown"
        }
      }
    }
  }
  //附加和分离新选项卡
  test("attaching and detaching a new tab") {
    withSpark(newSparkContext()) { sc =>
      val sparkUI = sc.ui.get

      val newTab = new WebUITab(sparkUI, "foo") {
        attachPage(new WebUIPage("") {
          def render(request: HttpServletRequest): Seq[Node] = {
            <b>"html magic"</b>
          }
        })
      }
      sparkUI.attachTab(newTab)
      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        goToUi(sc, "")
        find(cssSelector("""ul li a[href*="jobs"]""")) should not be(None)
        find(cssSelector("""ul li a[href*="stages"]""")) should not be(None)
        find(cssSelector("""ul li a[href*="storage"]""")) should not be(None)
        find(cssSelector("""ul li a[href*="environment"]""")) should not be(None)
        find(cssSelector("""ul li a[href*="foo"]""")) should not be(None)
      }
      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        // check whether new page exists
        //检查新页面是否存在
        goToUi(sc, "/foo")
        find(cssSelector("b")).get.text should include ("html magic")
      }
      sparkUI.detachTab(newTab)
      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        goToUi(sc, "")
        find(cssSelector("""ul li a[href*="jobs"]""")) should not be(None)
        find(cssSelector("""ul li a[href*="stages"]""")) should not be(None)
        find(cssSelector("""ul li a[href*="storage"]""")) should not be(None)
        find(cssSelector("""ul li a[href*="environment"]""")) should not be(None)
        find(cssSelector("""ul li a[href*="foo"]""")) should be(None)
      }
      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        // check new page not exist
        //检查新页面不存在
        goToUi(sc, "/foo")
        find(cssSelector("b")) should be(None)
      }
    }
  }
  //杀死阶段POST / GET响应是正确的
  test("kill stage POST/GET response is correct") {
    //获得响应代码
    def getResponseCode(url: URL, method: String): Int = {
      val connection = url.openConnection().asInstanceOf[HttpURLConnection]
      connection.setRequestMethod(method)
      connection.connect()
      val code = connection.getResponseCode()
      connection.disconnect()
      code
    }

    withSpark(newSparkContext(killEnabled = true)) { sc =>
      sc.parallelize(1 to 10).map{x => Thread.sleep(10000); x}.countAsync()
      eventually(timeout(5 seconds), interval(50 milliseconds)) {
        //http://192.168.100.227:43336===http://192.168.100.227:43336
        //去掉<string>字串中结尾的字符
       println(sc.ui.get.appUIAddress+"==="+sc.ui.get.appUIAddress.stripSuffix("/"))
        //http://192.168.100.227:43336/stages/stage/kill/?id=0&terminate=true
        val url = new URL(
          //stripSuffix去掉<string>字串中结尾的字符
          sc.ui.get.appUIAddress.stripSuffix("/") + "/stages/stage/kill/?id=0&terminate=true")
        // SPARK-6846: should be POST only but YARN AM doesn't proxy POST
        //SPARK-6846：应该是POST，但是YARN AM不代理POST
        println("==="+url)
        getResponseCode(url, "GET") should be (200)
        getResponseCode(url, "POST") should be (200)
      }
    }
  }
  //阶段和工作保留
  test("stage & job retention") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set("spark.ui.enabled", "true")
      //应用程序webUI的端口
      .set("spark.ui.port", "0")
      //在GC之前webUI保留的stage数量
      .set("spark.ui.retainedStages", "3")
      .set("spark.ui.retainedJobs", "2")
    val sc = new SparkContext(conf)
    assert(sc.ui.isDefined)

    withSpark(sc) { sc =>
      // run a few jobs & stages ...
      //运行几个工作和阶段...
      (0 until 5).foreach { idx =>
        // NOTE: if we reverse the order, things don't really behave nicely
        // we lose the stage for a job we keep, and then the job doesn't know
        // about its last stage
        //注意：如果我们扭转顺序，事情的行为不会很好
        //我们失去了我们保留的工作的阶段，然后工作不知道关于最后阶段
        sc.parallelize(idx to (idx + 3)).map(identity).groupBy(identity).map(identity)
          .groupBy(identity).count()
        sc.parallelize(idx to (idx + 3)).collect()
      }

      val expJobInfo = Seq(
        ("9", "collect"),
        ("8", "count")
      )

      eventually(timeout(1 second), interval(50 milliseconds)) {
        goToUi(sc, "/jobs")
        // The completed jobs table should have two rows. The first row will be the most recent job:
        find("completed-summary").get.text should be ("Completed Jobs: 10, only showing 2")
        find("completed").get.text should be ("Completed Jobs (10, only showing 2)")
        val rows = findAll(cssSelector("tbody tr")).toIndexedSeq.map{_.underlying}
        rows.size should be (expJobInfo.size)
        for {
          (row, idx) <- rows.zipWithIndex
          columns = row.findElements(By.tagName("td"))
          id = columns(0).getText()
          description = columns(1).getText()
        } {
          id should be (expJobInfo(idx)._1)
          description should include (expJobInfo(idx)._2)
        }
      }

      val jobsJson = getJson(sc.ui.get, "jobs")
      jobsJson.children.size should be (expJobInfo.size)
      for {
        (job @ JObject(_), idx) <- jobsJson.children.zipWithIndex
        id = (job \ "jobId").extract[String]
        name = (job \ "name").extract[String]
      } {
        withClue(s"idx = $idx; id = $id; name = ${name.substring(0, 20)}") {
          id should be (expJobInfo(idx)._1)
          name should include (expJobInfo(idx)._2)
        }
      }

      // what about when we query for a job that did exist, but has been cleared?
      //当我们查询确实存在但已被清除的工作时呢？
      goToUi(sc, "/jobs/job/?id=7")
      find("no-info").get.text should be ("No information to display for job 7")

      val badJob = HistoryServerSuite.getContentAndCode(apiUrl(sc.ui.get, "jobs/7"))
      badJob._1 should be (HttpServletResponse.SC_NOT_FOUND)
      badJob._2 should be (None)
      badJob._3 should be (Some("unknown job: 7"))

      val expStageInfo = Seq(
        ("19", "collect"),
        ("18", "count"),
        ("17", "groupBy")
      )

      eventually(timeout(1 second), interval(50 milliseconds)) {
        goToUi(sc, "/stages")
        find("completed-summary").get.text should be ("Completed Stages: 20, only showing 3")
        find("completed").get.text should be ("Completed Stages (20, only showing 3)")
        val rows = findAll(cssSelector("tbody tr")).toIndexedSeq.map{_.underlying}
        rows.size should be (3)
        for {
          (row, idx) <- rows.zipWithIndex
          columns = row.findElements(By.tagName("td"))
          id = columns(0).getText()
          description = columns(1).getText()
        } {
          id should be (expStageInfo(idx)._1)
          description should include (expStageInfo(idx)._2)
        }
      }

      val stagesJson = getJson(sc.ui.get, "stages")
      stagesJson.children.size should be (3)
      for {
        (stage @ JObject(_), idx) <- stagesJson.children.zipWithIndex
        id = (stage \ "stageId").extract[String]
        name = (stage \ "name").extract[String]
      } {
        id should be (expStageInfo(idx)._1)
        name should include (expStageInfo(idx)._2)
      }

      // nonexistent stage

      goToUi(sc, "/stages/stage/?id=12&attempt=0")
      find("no-info").get.text should be ("No information to display for Stage 12 (Attempt 0)")
      val badStage = HistoryServerSuite.getContentAndCode(apiUrl(sc.ui.get, "stages/12/0"))
      badStage._1 should be (HttpServletResponse.SC_NOT_FOUND)
      badStage._2 should be (None)
      badStage._3 should be (Some("unknown stage: 12"))

      val badAttempt = HistoryServerSuite.getContentAndCode(apiUrl(sc.ui.get, "stages/19/15"))
      badAttempt._1 should be (HttpServletResponse.SC_NOT_FOUND)
      badAttempt._2 should be (None)
      badAttempt._3 should be (Some("unknown attempt for stage 19.  Found attempts: [0]"))

      val badStageAttemptList = HistoryServerSuite.getContentAndCode(
        apiUrl(sc.ui.get, "stages/12"))
      badStageAttemptList._1 should be (HttpServletResponse.SC_NOT_FOUND)
      badStageAttemptList._2 should be (None)
      badStageAttemptList._3 should be (Some("unknown stage: 12"))
    }
  }
  //live UI json应用程序列表
  test("live UI json application list") {
    withSpark(newSparkContext()) { sc =>
      val appListRawJson = HistoryServerSuite.getUrl(new URL(
        sc.ui.get.appUIAddress + "/api/v1/applications"))
      val appListJsonAst = JsonMethods.parse(appListRawJson)
      appListJsonAst.children.length should be (1)
      val attempts = (appListJsonAst \ "attempts").children
      attempts.size should be (1)
      (attempts(0) \ "completed").extract[Boolean] should be (false)
      parseDate(attempts(0) \ "startTime") should be (sc.startTime)
      parseDate(attempts(0) \ "endTime") should be (-1)
      val oneAppJsonAst = getJson(sc.ui.get, "")
      oneAppJsonAst should be (appListJsonAst.children(0))
    }
  }

  def goToUi(sc: SparkContext, path: String): Unit = {
    goToUi(sc.ui.get, path)
  }

  def goToUi(ui: SparkUI, path: String): Unit = {
    //stripSuffix去掉<string>字串中结尾的字符
    go to (ui.appUIAddress.stripSuffix("/") + path)
  }

  def parseDate(json: JValue): Long = {
    JacksonMessageWriter.makeISODateFormat.parse(json.extract[String]).getTime
  }

  def getJson(ui: SparkUI, path: String): JValue = {
    JsonMethods.parse(HistoryServerSuite.getUrl(apiUrl(ui, path)))
  }

  def apiUrl(ui: SparkUI, path: String): URL = {
    new URL(ui.appUIAddress + "/api/v1/applications/test/" + path)
  }
}
