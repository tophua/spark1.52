package scalaDemo

/**
  * Created by liush on 17-7-19.
  */
object ReplaceAllTest {
  def main(args: Array[String]): Unit = {
    val cases = Seq(
      "application list json" -> "applications",
      "completed app list json" -> "applications?status=completed",
      "running app list json" -> "applications?status=running",
      "minDate app list json" -> "applications?minDate=2015-02-10",
      "maxDate app list json" -> "applications?maxDate=2015-02-10",
      "maxDate2 app list json" -> "applications?maxDate=2015-02-03T16:42:40.000GMT",
      "one app json" -> "applications/local-1422981780767",
      "one app multi-attempt json" -> "applications/local-1426533911241",
      "job list json" -> "applications/local-1422981780767/jobs",
      "job list from multi-attempt app json(1)" -> "applications/local-1426533911241/1/jobs",
      "job list from multi-attempt app json(2)" -> "applications/local-1426533911241/2/jobs",
      "one job json" -> "applications/local-1422981780767/jobs/0",
      "succeeded job list json" -> "applications/local-1422981780767/jobs?status=succeeded",
      "succeeded&failed job list json" ->
        "applications/local-1422981780767/jobs?status=succeeded&status=failed",
      "executor list json" -> "applications/local-1422981780767/executors",
      "stage list json" -> "applications/local-1422981780767/stages",
      "complete stage list json" -> "applications/local-1422981780767/stages?status=complete",
      "failed stage list json" -> "applications/local-1422981780767/stages?status=failed",
      "one stage json" -> "applications/local-1422981780767/stages/1",
      "one stage attempt json" -> "applications/local-1422981780767/stages/1/0",

      "stage task summary w shuffle write"
        -> "applications/local-1430917381534/stages/0/0/taskSummary",
      "stage task summary w shuffle read"
        -> "applications/local-1430917381534/stages/1/0/taskSummary",
      "stage task summary w/ custom quantiles" ->
        "applications/local-1430917381534/stages/0/0/taskSummary?quantiles=0.01,0.5,0.99",

      "stage task list" -> "applications/local-1430917381534/stages/0/0/taskList",
      "stage task list w/ offset & length" ->
        "applications/local-1430917381534/stages/0/0/taskList?offset=10&length=50",
      "stage task list w/ sortBy" ->
        "applications/local-1430917381534/stages/0/0/taskList?sortBy=DECREASING_RUNTIME",
      "stage task list w/ sortBy short names: -runtime" ->
        "applications/local-1430917381534/stages/0/0/taskList?sortBy=-runtime",
      "stage task list w/ sortBy short names: runtime" ->
        "applications/local-1430917381534/stages/0/0/taskList?sortBy=runtime",

      "stage list with accumulable json" -> "applications/local-1426533911241/1/stages",
      "stage with accumulable json" -> "applications/local-1426533911241/1/stages/0/0",
      "stage task list from multi-attempt app json(1)" ->
        "applications/local-1426533911241/1/stages/0/0/taskList",
      "stage task list from multi-attempt app json(2)" ->
        "applications/local-1426533911241/2/stages/0/0/taskList",

      "rdd list storage json" -> "applications/local-1422981780767/storage/rdd",
      "one rdd storage json" -> "applications/local-1422981780767/storage/rdd/0"
    )
    cases.foreach { case (name, path) =>{
      println(name+"==="+sanitizePath(name))
    }
    }

  }
  //消毒路径
  def sanitizePath(path: String): String = {
    // this doesn't need to be perfect, just good enough to avoid collisions
    //这不需要是完美的,只是足够好以避免碰撞
    //\W：匹配任何非单词字符
    path.replaceAll("\\W", "_")
  }

}
