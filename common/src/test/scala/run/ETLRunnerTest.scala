package run

import org.specs2.mutable.Specification
import util.{ApplicationConfig, InputSourceConfiguration, OutputSourceConfiguration}
import org.specs2.mock.Mockito

class ETLRunnerTest extends Specification with Mockito {

  sequential
  "method runETLJob" should {
    val applicationConfig = mock[ApplicationConfig]
    "throw excepiton " in {
      ETLRunner.runETLJob("", "") must throwA[Exception]
    }
    "should call extract transform and load metod" in {
      doReturn(Some(mock[InputSourceConfiguration])).when(applicationConfig).getInputSource()
      doReturn(Some(mock[OutputSourceConfiguration])).when(applicationConfig).getOutPut()
      ETLRunner.runETLJob("/conf/test.conf", "etl.TestEtlBuilder") mustEqual ("SUCCESS")
    }
  }
}
