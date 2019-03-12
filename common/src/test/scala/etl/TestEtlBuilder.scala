package etl

import org.apache.spark.sql.SparkSession

class TestEtlBuilder(confPath: String) extends SubETLBuilder(confPath: String) {
  override def extract()(implicit sparkSession: SparkSession): String = {
    "TestEtlBuilder extract"
  }

  override def transform()(implicit sparkSession: SparkSession): String = {
    "TestEtlBuilder transform"
  }

  override def load()(implicit sparkSession: SparkSession): String = {
    "TestEtlBuilder load"
  }
}
