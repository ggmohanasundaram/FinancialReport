package util

import com.typesafe.config.{Config, ConfigFactory}

import scala.io.Source
import scala.util.Try

object ApplicationConfig {
  def init(confPath: String): ApplicationConfig = {
    val confURL = getClass.getResource(confPath)
    val confString = scala.io.Source.fromURL(confURL)
    new ApplicationConfig(confString.mkString)
  }

  def init(confPath: String, confType: String = "Internal"): ApplicationConfig = {
    val confString = Source.fromFile(confPath)
    new ApplicationConfig(confString.mkString)
  }
}

class ApplicationConfig(confString: String) {

  private val config: Config = ConfigFactory.parseString(confString)

  def getInputSource(): Option[InputSourceConfiguration] = {
    implicit val contextPath: String = "run.InputSource"
    val connectionURL = Try(config.getString(s"$contextPath.connectionURL")).toOption
    val userName = Try(config.getString(s"$contextPath.userName")).toOption
    val password = Try(config.getString(s"$contextPath.password")).toOption
    val inputPath = Try(config.getString(s"$contextPath.inputPath")).toOption
    val inputFormat = Try(config.getString(s"$contextPath.inputFormat")).toOption
    Some(InputSourceConfiguration(connectionURL, userName, password, inputPath, inputFormat))
  }

  def getOutPut(): Option[OutputSourceConfiguration] = {
    implicit val contextPath: String = "run.OutputSource"
    val connectionURL = Try(config.getString(s"$contextPath.connectionURL")).toOption
    val userName = Try(config.getString(s"$contextPath.userName")).toOption
    val password = Try(config.getString(s"$contextPath.password")).toOption
    val outputPath = Try(config.getString(s"$contextPath.outputPath")).toOption
    val outputFormat = Try(config.getString(s"$contextPath.outputFormat")).toOption
    Some(OutputSourceConfiguration(connectionURL, userName, password, outputPath, outputFormat))
  }
}

case class InputSourceConfiguration(connectionURL: Option[String], userName: Option[String], password: Option[String], inputPath: Option[String], inputFormat: Option[String])

case class OutputSourceConfiguration(connectionURL: Option[String], userName: Option[String], password: Option[String], outputPath: Option[String], outputFormat: Option[String])