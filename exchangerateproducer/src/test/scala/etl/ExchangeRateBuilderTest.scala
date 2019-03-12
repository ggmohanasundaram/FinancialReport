package etl

import org.specs2.mock.Mockito
import util.SparkIo
import util.testutils.TestUtils

class ExchangeRateBuilderTest extends TestUtils with Mockito {

  sequential

  val testJson =
    """
      |{
      |  "disclaimer": "Usage subject to terms: https://openexchangerates.org/terms",
      |  "license": "https://openexchangerates.org/license",
      |  "timestamp": 1552021201,
      |  "base": "USD",
      |  "rates": {
      |    "AED": 3.673181,
      |    "AFN": 74.998542,
      |    "ALL": 110.593593,
      |    "AMD": 488.463133,
      |    "ANG": 1.814845,
      |    "AOA": 314.62,
      |    "ARS": 42.438,
      |    "AUD": 1.42687
      |    }
      |  }
    """.stripMargin

  "Method extract in ExchangeRateBuilderBuilder" should {
    "Process  write the json file" in {
      val sparkIo = spy(new SparkIo)
      val builder = spy(new ExchangeRateBuilder("/conf/exchangerate_test.conf"))
      doReturn(sparkIo).when(builder).getSparkIo
      doReturn((200, Right(testJson))).when(builder).getExchangeRate(anyString)
      val outputPath = builder.extract()(sparkSession)
      val frame = new SparkIo().loadJsonFromPath(outputPath)
      frame.count() mustEqual (1)
    }
  }

  "Method transform in ExchangeRateBuilderBuilder" should {
    "transform  and load the spark view" in {
      val sparkIo = spy(new SparkIo)
      val builder = spy(new ExchangeRateBuilder("/conf/exchangerate_test.conf"))
      builder.outputPath = "/media/sf_SharedFolder/FinancialReport/testResults/testOutPut/openexchangerates/raw/ExchangeRate/d201903121411"
      val input = builder.transform()(sparkSession)
      val dataFrame = sparkSession.sql(s"select * from $input")
      dataFrame.count() mustEqual (8)
      dataFrame.schema.toList.size mustEqual(3)
    }
  }

  "Method load in ExchangeRateBuilderBuilder" should {
    "write the orc file" in {
      val sparkIo = spy(new SparkIo)
      val builder = spy(new ExchangeRateBuilder("/conf/exchangerate_test.conf"))
           import sparkSession.implicits._
      val dataFrame = Seq(("a","b","c"),("a","b","c")).toDF()
      dataFrame.createOrReplaceTempView(builder.inputTableName)
      val path = builder.load()(sparkSession)
      val output = new SparkIo().readOrc(path)
      output.count() mustEqual (2)
    }
  }

}
