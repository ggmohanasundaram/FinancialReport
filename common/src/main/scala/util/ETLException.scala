package util

case class ETLException(message: String = "") extends Exception(message)
