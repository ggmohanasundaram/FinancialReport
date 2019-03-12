package etl

import com.softwaremill.sttp.{sttp, _}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import util.ETLUtils
import org.apache.spark.sql.functions._

class ExchangeRateBuilder(confPath: String,confType:String="Internal") extends SubETLBuilder(confPath: String,confType=confType:String) {

  private val log = LoggerFactory.getLogger(classOf[ExchangeRateBuilder])

  val sparkIo = getSparkIo
  var outputPath = ""
  val inputTableName = "ExchangeRate"
  val inputSource = applicationConfig.getInputSource().get
  val outputSource = applicationConfig.getOutPut().get

  override def extract()(implicit sparkSession: SparkSession): String = {
    try {
      log.info(s"get Exchange Rate Info from ${inputSource.connectionURL.get}")
      val response = getExchangeRate(s"${inputSource.connectionURL.get}?app_id=${inputSource.password.get}")
      response._1 match {
        case 200 =>
          val value = response._2
          val exchangeRate = sparkIo.loadJsonFromString(value.right.get)
          outputPath = s"${outputSource.outputPath.get}/openexchangerates/raw/$inputTableName/"
          sparkIo.writeJson(exchangeRate, outputPath)
        case _ => log.info("No response From openexchangerates")
      }
      outputPath
    }
    catch {
      case e: Exception => throw e
    }
  }

  def converDataType(input: Any) = {
    input  match {
      case input if input.isInstanceOf[Double]=> input.asInstanceOf[Double]
      case input if input.isInstanceOf[Long] => input.asInstanceOf[Long].toDouble
      case _ => 0.00
    }
  }

  override def transform()(implicit sparkSession: SparkSession): String = {
    try {
      import sparkSession.implicits._
      val dataFrame = sparkIo.loadJsonFromPath(outputPath)
      val conversionRateDf = dataFrame.select("rates.*")
      val currencyDataFrame = conversionRateDf.schema.map(x => x.name)
      val mergedDf = conversionRateDf.collect().flatMap(x => x.getValuesMap(currencyDataFrame))

      val transformedDf = mergedDf.map(x => (x._1, converDataType(x._2))).toList.toDF("currency", "conversion_rate")
      val updatedTime = ETLUtils.getDatePath
      val finalDataFrame = transformedDf.withColumn("UpdatedDate", typedLit(updatedTime))
      finalDataFrame.createOrReplaceTempView(inputTableName)
      inputTableName
    } catch {
      case e: Exception => throw e
    }
  }

  def getExchangeRate(urlString: String) = {
    try {
      val sort: Option[String] = None
      val query = "http language:scala"
      sttp.get(uri"${urlString}")
      implicit val backend = HttpURLConnectionBackend()
      val request = sttp.get(uri"$urlString")
      val response = request.send()
      (response.code, response.body)
    }
    catch {
      case e: Exception => throw e
    }
  }

  override def load()(implicit sparkSession: SparkSession): String = {
    try {
      val dataFrame = sparkSession.sql(s"select * from $inputTableName")
      val outputPath = s"${outputSource.outputPath.get}/processed/$inputTableName/"
      sparkIo.writeOrc(dataFrame, outputPath, List("UpdatedDate"), saveMode = SaveMode.Append)
    }
    catch {
      case e: Exception => throw e
    }
  }
}
