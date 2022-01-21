package staticVirality
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object ViralityRunner extends App {
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
    val transformed = df.filter($"_c2" !== "null").filter($"_c4" !== "PROBLEM")
      .select(col("_c1").as("id").cast("Long"),
        col("_c2").as("cascade").cast("Long")
        ,col("_c3").as("depth").cast("Int"),
        col("_c4").as("hateful"))
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

  //----------------------------------------------------------------------------------------------------
  //----------------------------------------------------------------------------------------------------
  // ALGORITHMS
  //----------------------------------------------------------------------------------------------------
  //----------------------------------------------------------------------------------------------------

  /**
   * Generates the dataframe with original virality formula: The one that appears in Goel et all paper
   *
   */
  def viralityFormula(dataset: DataFrame): DataFrame = {

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

    viralityResult
  }

  /**
   * Effective Branching Number. Average number of children per generation
   *
   */
    def avgChildrenPerGen(dataset: DataFrame): (DataFrame, DataFrame) = {

      // Select a filter for cascades
      val hated = filterFirstPost(dataset)

      // Change hate column by the results of the former filter
      val hateApplied = dataset.drop("hateful").join(hated,"cascade")

      // Group by depth for both hateful and non-hateful
      val hatefulGenerations = hateApplied.filter($"hateful" === true)
        .groupBy("depth").count
      val nonHatefulGenerations = hateApplied.filter($"hateful" === false)
        .groupBy("depth").count

      // Prepare DataFrames for the formula
      val hatefulPreBranching = hatefulGenerations.filter($"depth" !== 0)
        .withColumn("depth",col("depth")-lit(1))
        .withColumnRenamed("count","children")
      val nonHatefulPreBranching = nonHatefulGenerations.filter($"depth" !== 0)
        .withColumn("depth",col("depth")-lit(1))
        .withColumnRenamed("count","children")

      // Results
      val hatefulResult = hatefulGenerations.join(hatefulPreBranching,"depth")
        .withColumn("EBN",col("children")/col("count")).orderBy("depth");
      val nonHatefulResult = nonHatefulGenerations.join(nonHatefulPreBranching,"depth")
        .withColumn("EBN",col("children")/col("count")).orderBy("depth");

      (hatefulResult,nonHatefulResult)
    }

  //----------------------------------------------------------------------------------------------------
  //----------------------------------------------------------------------------------------------------
  // SAVE RESULTS SECTION
  //----------------------------------------------------------------------------------------------------
  //----------------------------------------------------------------------------------------------------

  def writeResults(dataFrame: DataFrame, path: String, format: String): Unit = format match {
    case "json" =>
      dataFrame.write.json(path)
    case "csv" =>
      dataFrame.write.csv(path)
    case _ =>
      println("INCORRECT FORMAT")
  }

  //----------------------------------------------------------------------------------------------------
  //----------------------------------------------------------------------------------------------------
  // EXECUTION SECTION
  //----------------------------------------------------------------------------------------------------
  //----------------------------------------------------------------------------------------------------

  val dataset = this.getData("/home/rcalzada/DepthFromOriginal_1642673382044")

  // Virality
  val viralityResult = viralityFormula(dataset)

  // Generations
  val hatefulResult = avgChildrenPerGen(dataset)._1
  val nonHatefulResult = avgChildrenPerGen(dataset)._2

  // Save results in a file
  writeResults(viralityResult,"/home/rcalzada/output/virality_8_nt","csv")
  writeResults(hatefulResult,
    "/home/rcalzada/output/generations_8part_nt/hateful","csv")
  writeResults(nonHatefulResult,
    "/home/rcalzada/output/generations_8part_nt/non-hateful","csv")
}

