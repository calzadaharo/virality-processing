package staticVirality
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object VitalityRunner extends App {
  val spark : SparkSession = SparkSession.builder
    .appName("Data ingestion")
    .master("local[*]")
    .getOrCreate();

  import spark.implicits._

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
   * Filter cascades by the content of the first post
   *
   */
  def filterFirstPost(dataset: DataFrame): DataFrame = {
    dataset.filter($"depth"===0).select("cascade","hateful")
  }

  /**
   * Generates the dataframe with original virality formula: The one that appears in Goel et all paper
   *
   */
  def viralityFormula(dataset: DataFrame): Unit = {
    // First, a new column from 1 to depth is generated for each of the posts with depth!= 0. After that,
    // content is expanded in different rows for the future summatory
    var sumTerms = dataset.filter($"depth" !== 0).withColumn("listed",
      sequence(lit(1), col("depth")))
    sumTerms=sumTerms.withColumn("explosion",explode(col("listed")))

    // Count the number of posts per cascade
    val counting = dataset.groupBy("cascade").count

    // Select the filter for the cascades
    val hated = filterFirstPost(dataset)

    // Generate the final dataset
    val grouped = sumTerms.groupBy("cascade")
    var previous = grouped.agg(sum("explosion") as "totalSum")
    previous = previous.join(counting,"cascade")
    previous = previous.join(hated,"cascade")

    val viralityResult = previous.withColumn("virality",
      (lit(1)/(col("count")*(col("count")-lit(1))))
        *col("totalSum"))

    viralityResult.show()
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
