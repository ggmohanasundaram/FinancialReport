package run

import java.io.File
import java.net.{URL, URLClassLoader}

import etl.SubETLBuilder
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import util.{ETLException, SparkIo}
import util.RunnerUtil.nextAdditionalOptions

object ETLRunner {

  private val log = LoggerFactory.getLogger(ETLRunner.getClass)

  def main(args: Array[String]): Unit = {
    log.info("--ETLRunner Starts-- ")
    try {
      val additionalOptions = nextAdditionalOptions(Map(), args.toList)
      val confPath = additionalOptions('confPath)
      val confType = additionalOptions('confType)
      val jarpath = additionalOptions('jarPath)
      val etlAppName = additionalOptions('etlAppName)
      runETLJob(confPath, etlAppName, confType,jarpath)
    }
    catch {
      case e: Exception => log.error(s"Exception in runETLJob ${ExceptionUtils.getStackTrace(e)}")
    }
  }

  def runETLJob(confPath: String, etlAppName: String, confType: String = "Internal",jarPath:String=""): String = {

    val sparkIo = new SparkIo()
    implicit val sparkSession: SparkSession = sparkIo.createSparkSession(etlAppName)
    try {
      log.info(s"ETLRunner.runETLJob Starts--$etlAppName")

      if(!jarPath.isEmpty){
        var classLoader = new URLClassLoader(Array[URL](
          new File(jarPath).toURI.toURL))
        var clazz = classLoader.loadClass(etlAppName)
      }
      val builderClass = Class.forName(etlAppName)
      val etlBuilder = builderClass.getConstructor(classOf[String], classOf[String]).newInstance(confPath, confType).asInstanceOf[SubETLBuilder]
      etlBuilder.extract()
      etlBuilder.transform()
      etlBuilder.load()
      "SUCCESS"
    }
    catch {
      case e: Exception => log.error(s"Exception in runETLJob ${ExceptionUtils.getStackTrace(e)}")
        throw ETLException(ExceptionUtils.getStackTrace(e))

    }
    finally {
      sparkSession.stop()
    }
  }
}
