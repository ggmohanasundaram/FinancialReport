package e2erun

import run.ETLRunner

object E2ERun {
  def main(args: Array[String]): Unit = {
    val parrentPath = s"${System.getProperty("user.dir")}/e2e/src/main/resources/conf"

    ETLRunner.runETLJob(s"$parrentPath/exchangerate_openexchangerates.conf","etl.ExchangeRateBuilder","External",
      jarPath = s"$parrentPath/exchangerateproducer/target/scala-2.11/exchangerateproducer_2.11-0.1.0-SNAPSHOT.jar")

    ETLRunner.runETLJob(s"$parrentPath/report_conf.conf","etl.ReportETLBuilder","External",
      jarPath = s"$parrentPath/report/target/scala-2.11/report_2.11-0.1.0-SNAPSHOT.jar")
  }

}
