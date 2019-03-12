package util

import org.specs2.mutable.Specification
import util.testutils.TestUtils


class ApplicationConfigTest extends Specification {

  "ApplicationConfig" should {
    "process input source configuration" in {
      val applicationConfig = ApplicationConfig.init("/conf/test.conf")
      applicationConfig.getInputSource().get mustEqual (InputSourceConfiguration(Some("someConnectionURL"), Some("someUserName"), Some("somePassword"), Some("someInputPath"),Some("someinputFormat")))
    }
    "process output source configuration" in {
      val applicationConfig = ApplicationConfig.init("/conf/test.conf")
      applicationConfig.getOutPut().get mustEqual (OutputSourceConfiguration(Some("someConnectionURL"), Some("someUserName"), Some("somePassword"), Some("someoutputPath"), Some("someoutPutFormat")))
    }
    "process output source configuration from file system" in {
      val parrentPath = System.getProperty("user.dir")
      val applicationConfig = ApplicationConfig.init(s"$parrentPath/common/src/test/resources/conf/test.conf","")
      applicationConfig.getOutPut().get mustEqual (OutputSourceConfiguration(Some("someConnectionURL"), Some("someUserName"), Some("somePassword"), Some("someoutputPath"), Some("someoutPutFormat")))
    }
    "process input source configuration from file system" in {
      val parrentPath = System.getProperty("user.dir")
      val applicationConfig = ApplicationConfig.init(s"$parrentPath/common/src/test/resources/conf/test.conf","")
      applicationConfig.getInputSource().get mustEqual (InputSourceConfiguration(Some("someConnectionURL"), Some("someUserName"), Some("somePassword"), Some("someInputPath"),Some("someinputFormat")))
    }
  }
}
