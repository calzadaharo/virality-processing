package staticVirality
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object VitalityRunner extends App {
  val spark : SparkSession = SparkSession.builder
    .appName("Data ingestion")
    .master("local[*]")
    .getOrCreate();

  import spark.implicits._;

  /**
   * Creates the dataframe from the dataset provided
   *
   */
  def getData(filename: String): DataFrame = {
    val df = spark.read.csv(filename)
    val transformated = df.filter($"_c3" !== "null")
      .select(col("_c1").as("id").cast("Long"),
        col("_c2").as("cascade"),col("_c3").as("depth").cast("Int"),
        col("_c4").as("hateful"))
    transformated
  }

  /**
   * Generates the dataframe with original virality formula: The one that appears in Goel et all paper
   *
   */
  def viralityFormula(dataset: DataFrame): Unit = {
    var sumTerms = dataset.filter($"depth" === 0).withColumn("listed",
      sequence(lit(0), col("depth")))
    sumTerms=sumTerms.withColumn("explosion",explode(col("listed")))

//    var sumTerms = dataset.withColumn("listed",
//      sequence(lit(0), col("depth")))
//    sumTerms=sumTerms.withColumn("explosion",explode(col("listed")))

    val counting = dataset.groupBy("cascade").count

    dataset.show()
    sumTerms.show()
    counting.show()
  }

  //----------------------------------------------------------------------------------------------------
  //----------------------------------------------------------------------------------------------------
  // EXECUTION SECTION
  //----------------------------------------------------------------------------------------------------
  //----------------------------------------------------------------------------------------------------
  val dataset = this.getData("src/main/scala/DepthFromOriginal_small/partition-0")

  // Virality
  viralityFormula(dataset)
}

