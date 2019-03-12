package util

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

class SparkIo {

  private val logger = LoggerFactory.getLogger(classOf[SparkIo])

  def writeOrc[T](dataset: Dataset[T], filePath: String, primaryKeys: List[String], repartition: Boolean = false, saveMode: SaveMode = SaveMode.Overwrite, schemaCaseType: String = "lower")
                 (implicit sparkSession: SparkSession): String = {
    logger.info(s"Writing ORC files at $filePath with saveMode=$saveMode")
    try {
      val repartitionedDataset = if (repartition)
        dataset.repartition(primaryKeys.map(col): _*)
      else dataset

      repartitionedDataset.write
        .mode(saveMode)
        .orc(filePath)

      filePath
    } catch {
      case e: Exception => throw ETLException(s"Error while writing ORC $filePath with saveMode=$saveMode => ${e.getMessage}")
    }
  }

  def loadJsonFromString(jsonInput: String)(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._
    sparkSession.sqlContext.read.option("multiline", "true").json(Seq(jsonInput).toDS())
  }

  def readOrc(inputPath: String)(implicit sparkSession: SparkSession) = {
    logger.info(s"read orc file from $inputPath")
    sparkSession.sqlContext.read.orc(inputPath)
  }

  def readCsv(inputPath: String)(implicit sparkSession: SparkSession) = {
    logger.info(s"read orc file from $inputPath")
    sparkSession.sqlContext.read.option("header", true)
      .option("delimiter", "|").csv(inputPath)
  }

  def loadJsonFromPath(path: String)(implicit sparkSession: SparkSession) = {
    sparkSession.sqlContext.read.option("multiline", "true").json(path)
  }

  def writeJson[T](dataset: Dataset[T], filePath: String)(implicit sparkSession: SparkSession) = {
    dataset.write.mode(SaveMode.Overwrite).json(filePath)
  }

  def createSparkSession(appName: String, sparkRuntimeConf: Map[String, String] = Map.empty): SparkSession = {
    SparkSession
      .builder()
      .appName(appName)
      .enableHiveSupport()
      .config("spark.sql.orc.enabled",true)
      .master("local[*]")
      .getOrCreate()

  }

}
