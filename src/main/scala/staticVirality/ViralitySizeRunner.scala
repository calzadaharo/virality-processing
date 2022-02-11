package staticVirality

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object ViralitySizeRunner {

  val spark : SparkSession = SparkSession.builder
    .appName("Data ingestion")
    .master("local[*]")
    .getOrCreate();

  import spark.implicits._

  //----------------------------------------------------------------------------------------------------
  //----------------------------------------------------------------------------------------------------
  // INGESTION
  //----------------------------------------------------------------------------------------------------
  //----------------------------------------------------------------------------------------------------

  /**
   * Creates the dataframe from the dataset provided
   *
   */
  def getData(filename: String): DataFrame = {
    val df = spark.read.csv(filename)
    val transformed = df.filter($"_c3" !== "null")
      .select(col("_c1").as("id").cast("Long"),
        col("_c3").as("cascade").cast("Long"),
        col("_c4").as("depth").cast("Int"),
        col("_c5").as("timestamp").cast("Int"),
        col("_c6").as("hateful"))
    transformed
  }

  //----------------------------------------------------------------------------------------------------
  //----------------------------------------------------------------------------------------------------
  // FILTERS
  //----------------------------------------------------------------------------------------------------
  //----------------------------------------------------------------------------------------------------

  /**
   * Filter cascades by the content of the first post
   *
   */
  def filterFirstPost(dataset: DataFrame): DataFrame = {
    dataset.filter($"depth"===0).select("cascade","hateful")
  }
}




