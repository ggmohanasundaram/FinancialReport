package util

import java.text.SimpleDateFormat
import java.util.Date

object ETLUtils {

  def getDatePath(): String = new SimpleDateFormat("yyyyMMddHHmm").format(new Date())

}
