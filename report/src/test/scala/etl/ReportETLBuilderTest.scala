package etl

import org.specs2.mock.Mockito
import util.testutils.TestUtils

class ReportETLBuilderTest extends TestUtils with Mockito {

  "Export Method in ReportETLBuilderTest" should {
    "load the input sources and create spark view" in {
      val builder = spy(new ReportETLBuilder("/conf/report_test.conf"))
      builder.extract()
      sparkSession.sql("select * from ExchangeRate").count() mustEqual (3)
      sparkSession.sql("select * from financialreport").count() mustEqual (3)
    }
  }

  "Method transform in ReportETLBuilderTest" should {
    "transform  and load the spark view" in {
      val builder = spy(new ReportETLBuilder("/conf/report_test.conf"))
      import sparkSession.implicits._
      Seq(("A","10.000","AUD"),("B","10.000","GBP"),("B","10.000","NZD")).toDF("NAME","TOTAL","CURRENCYTYPE")
        .createOrReplaceTempView("financialreport")
      Seq(("AUD",1.400),("GBP",0.99),("NZD",1.500)).toDF("currency", "conversion_rate")
        .createOrReplaceTempView("ExchangeRate")
      builder.transform()
      sparkSession.sql("select * from financeReport").count() mustEqual (3)
    }
  }

  "Method load in ReportETLBuilderTest" should {
    "should write orc file" in {
      val builder = spy(new ReportETLBuilder("/conf/report_test.conf"))
      import sparkSession.implicits._
      Seq(("A","10.000","AUD"),("B","10.000","GBP"),("B","10.000","NZD")).toDF("NAME","TOTAL","CURRENCYTYPE")
        .createOrReplaceTempView("financeReport")
       builder.load().mustEqual("testResults/testOutPut/report/financeReport/")
    }
  }
}
