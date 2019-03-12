package util

import org.slf4j.LoggerFactory

object RunnerUtil {

  private val logger = LoggerFactory.getLogger(RunnerUtil.getClass)

  type OptionMap = Map[Symbol, String]

  def nextAdditionalOptions(optionsMap: OptionMap, args: List[String]): OptionMap = {
    args match {
      case Nil => optionsMap
      case "--conf" :: value :: tail =>
        nextAdditionalOptions(optionsMap ++ Map('confPath -> value.toString), tail)
      case "--conf-type" :: value :: tail =>
        nextAdditionalOptions(optionsMap ++ Map('confType -> value.toString), tail)
      case "--etl-name" :: value :: tail =>
        nextAdditionalOptions(optionsMap ++ Map('etlAppName -> value.toString), tail)
      case "--jar-path" :: value :: tail =>
        nextAdditionalOptions(optionsMap ++ Map('jarPath -> value.toString), tail)
      case option :: _ => println("Unknown option " + option)
        sys.exit(1)
    }
  }
}