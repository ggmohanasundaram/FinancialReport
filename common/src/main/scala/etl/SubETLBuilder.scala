package etl

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import util.{ApplicationConfig, SparkIo}

abstract class SubETLBuilder(confPath: String,sparkIo:SparkIo = new SparkIo,confType:String="Internal") {

  private val log = LoggerFactory.getLogger(classOf[SubETLBuilder])

  log.info(s"confType - $confType")

  def getSparkIo = sparkIo

  val applicationConfig =  confType match {
    case "Internal" => ApplicationConfig.init(confPath)
    case "External" =>  ApplicationConfig.init(confPath,confType)
  }

  def extract()(implicit sparkSession: SparkSession): String

  def transform()(implicit sparkSession: SparkSession): String

  def load()(implicit sparkSession: SparkSession): String

}
