package staticVirality

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object ViralitySizeRunner {

  val spark : SparkSession = SparkSession.builder
    .appName("Data ingestion")
    .master("local[*]")
    .getOrCreate();

  import spark.implicits._

  
}




