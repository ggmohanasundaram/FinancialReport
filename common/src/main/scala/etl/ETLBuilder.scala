package etl

trait ETLBuilder {

  def extract():String

  def transform():String

  def load():String

}
