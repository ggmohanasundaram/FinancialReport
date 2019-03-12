package util.testutils

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import org.specs2.mutable.Specification
import org.specs2.specification.AfterAll

trait TestUtils extends Specification with AfterAll {

  val logger: Logger = LoggerFactory.getLogger(classOf[TestUtils])

  implicit lazy val sparkSession: SparkSession = {
    logger.info("Creating SparkSession for tests")

    SparkSession
      .builder()
      .appName(getClass.getName)
      .enableHiveSupport()
      .master("local[*]")
      .config("spark.sql.orc.enabled",true)
      .getOrCreate()
  }

  def afterAll: Unit = {
    logger.info("SparkSession stopped !")
    sparkSession.stop()
    logger.info(s"SparkSession is stopped: " + sparkSession.sparkContext.isStopped)
  }

}

