package etl

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

class ReportETLBuilder(confPath: String,confType:String="Internal") extends SubETLBuilder(confPath: String,confType=confType:String) {

  private val log = LoggerFactory.getLogger(classOf[ReportETLBuilder])
  val sparkIo = getSparkIo
  var outputPath = ""
  val inputSource = applicationConfig.getInputSource().get
  val outputSource = applicationConfig.getOutPut().get


  private val currency_Input = "ExchangeRate"
  private val financeReportInput = "financialreport"
  private val fiananceReportOutPut = "financeReport"

  override def extract()(implicit sparkSession: SparkSession): String = {
    try {
      val currency_rate_inputPath = s"${inputSource.inputPath.get}/processed/$currency_Input"
      val finance_inputPath = s"${inputSource.inputPath.get}/$financeReportInput"
      sparkIo.readOrc(currency_rate_inputPath).createOrReplaceTempView(currency_Input)
      sparkIo.readCsv(finance_inputPath).createOrReplaceTempView(financeReportInput)
      "SUCCESS"
    }
    catch {
      case e: Exception => throw e
    }
  }

  override def transform()(implicit sparkSession: SparkSession): String = {
    try {
      val report_sql =
        s"""
           |select fi.NAME,
           |case when fi.currencytype != 'AUD' THEN fi.TOTAL/ci.conversion_rate
           |else fi.TOTAL
           | end as total
           |from $financeReportInput fi join $currency_Input ci on fi.currencytype = ci.currency
       """.stripMargin

      val frame = sparkSession.sql(report_sql)
      frame.createOrReplaceTempView(fiananceReportOutPut)
      "SUCCESS"
    }
    catch {
      case e: Exception => throw e
    }
  }

  override def load()(implicit sparkSession: SparkSession): String = {
   try{
     val frame = sparkSession.sql(s"select * from $fiananceReportOutPut")
     val outputpath = s"${outputSource.outputPath.get}/report/$fiananceReportOutPut/"
     sparkIo.writeOrc(frame,outputpath,Nil)
   }
   catch {
     case e: Exception => throw e
   }
  }
}
