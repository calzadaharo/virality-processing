package staticVirality
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.typesafe.scalalogging.Logger
import Array._
import org.apache.spark.sql.expressions.Window

object ViralityRunner extends App {
  val spark : SparkSession = SparkSession.builder
    .appName("Virality")
    .getOrCreate();

  import spark.implicits._

  val logger = Logger("Root")

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

  /**
   * Creates the dataframe from the dataset provided
   *
   */
  def getDynamicData(filename: String): DataFrame = {
    val df = spark.read.csv(filename)
    val transformed = df.filter($"_c3" !== "null")
      .select(col("_c1").as("id").cast("Long"),
        col("_c3").as("cascade").cast("Long"),
        col("_c4").as("depth").cast("Int"),
        col("_c5").as("timestamp").cast("Int"),
        col("_c6").as("hateful"))
    logger.info("DATASET GENERATED")
    transformed
  }

  //----------------------------------------------------------------------------------------------------
  //----------------------------------------------------------------------------------------------------
  // FILTERS
  //----------------------------------------------------------------------------------------------------
  //----------------------------------------------------------------------------------------------------

  /**
   * Filter cascades by the content of the first post
   * Returns: Cascade-hateful
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
   * Generates the dataframe with original virality formula: The one that appears in Goel et all paper
   *
   */
  def viralityDynamicFormula(dataset: DataFrame, counting: DataFrame): DataFrame = {

    // Calculates sum of the series 0 ... n
    val zero2N = udf((n: Int) => n * (n + 1) / 2)

    var sumTerms = dataset.withColumn("totalSum", zero2N('depth))

    // Select the filter for the cascades
    val hated = filterFirstPost(dataset)

    // Generate the final dataset
    val grouped = sumTerms.groupBy("cascade")
    var previous = grouped.agg(sum("totalSum") as "totalSum")
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
  // DYNAMIC
  //----------------------------------------------------------------------------------------------------
  //----------------------------------------------------------------------------------------------------

  /**
   * WARNING: WHAT HAPPENS WHEN A CASCADE ENDS?
   *
   */
  def incrementalWindowExecution(bounds: (Int,Int), increment: Int, dataset: DataFrame): DataFrame = {
    val lowestBound = bounds._1
    val highestBound = bounds._2

    logger.info(s"FROM ${lowestBound} TO ${highestBound}")

    val cascadesTimestamp = dataset.groupBy("cascade").agg(
      max("timestamp").as("duration"))

    logger.info("CASCADES TIMESTAMP OK")

    val cascadesFiltered = cascadesTimestamp
      .filter($"duration" <= highestBound && $"duration" >= lowestBound)

    logger.info("CASCADES FILTERED OK")
    logger.info(s"Count:  ${cascadesFiltered.count()}")

    val filteredPosts = dataset.join(cascadesFiltered,"cascade")

    logger.info("FILTERED POSTS OK")

    var a: Int = 0;

    var viralityEvolution: DataFrame = cascadesFiltered.
      withColumn("size",col("duration")+lit(1)).
      select("cascade","size").cache()
//    var viralityEvolution: DataFrame = cascadesFiltered.select("cascade")

    logger.info("VIRALITY EVOLUTION OK")

    val counting = filteredPosts.groupBy("cascade").count.cache()

    val values = range(lowestBound,highestBound+1,increment)

    values.foreach(value => {
      a = value
      logger.info(s"Until ${value}")

      val partition = filteredPosts.filter($"timestamp" <= value)
      val result = viralityDynamicFormula(partition,counting).
        select("cascade", "virality").
        withColumnRenamed("virality","virality_"+value)
      viralityEvolution = viralityEvolution.join(result,"cascade").cache()
    })

//    for (i <- lowestBound to highestBound by increment) {
////    for (i <- lowestBound to 4 by increment) {
//
//      logger.info("Until " + i)
//      a = i
//
//      val partition = filteredPosts.filter($"timestamp" <= i)
//      val result = viralityDynamicFormula(partition,counting).
//        select("cascade", "virality").
//        withColumnRenamed("virality","virality_"+i)
//      viralityEvolution = viralityEvolution.join(result,"cascade")
//    }

    if (a != highestBound) {
      val partition = filteredPosts.filter($"timestamp" <= highestBound)
      val result = viralityFormula(partition).
        select("cascade", "virality").
        withColumnRenamed("virality","virality_" + highestBound)
      viralityEvolution = viralityEvolution.join(result,"cascade")
    }

    val cascadeHate = filterFirstPost(dataset)

    viralityEvolution = viralityEvolution.join(cascadeHate,"cascade")

//    viralityEvolution.show
    viralityEvolution
  }

  def efficientViralityEvolution(dataset: DataFrame): DataFrame = {
    val sum2n = udf((n:Int) =>n*(n+1)/2)
    val w = Window.partitionBy('cascade).orderBy('timestamp)
    val result = dataset.withColumn("d_sum", sum2n('depth)).
      withColumn("d_cumsum", sum('d_sum) over w).
      withColumn("virality", 'd_cumsum/(('timestamp+1)*('timestamp)))
    result
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
      logger.error("INCORRECT FORMAT")
  }

  //----------------------------------------------------------------------------------------------------
  //----------------------------------------------------------------------------------------------------
  // EXECUTION SECTION
  //----------------------------------------------------------------------------------------------------
  //----------------------------------------------------------------------------------------------------

  val dataset = this.getDynamicData(
    "hdfs://com31.dit.upm.es:9000/data/rcalzada/results/raphtory/DepthTimestampFromOriginal_static")
//  val dataset = this.getData(
//    "/home/rcalzada/static/DepthFromOriginal_1642673382044")

  // Virality

//  val viralityResult = viralityFormula(dataset)

  // Generations

//  val hatefulResult = avgChildrenPerGen(dataset)._1
//  val nonHatefulResult = avgChildrenPerGen(dataset)._2

  //Dynamic
  dataset.cache()
  val dynamicResult = efficientViralityEvolution(dataset)

  // Save results in a file

//  writeResults(viralityResult,"/home/rcalzada/output/test","csv")
//  writeResults(hatefulResult,
//    "/home/rcalzada/output/generations_8_nt/hateful","csv")
//  writeResults(nonHatefulResult,
//    "/home/rcalzada/output/generations_8_nt/non-hateful","csv")
    writeResults(dynamicResult,
      "hdfs://com31.dit.upm.es:9000/data/rcalzada/results/wholeViralityEvolution","csv")
//    writeResults(dynamicResult,
//      "hdfs://com31.dit.upm.es:9000/data/rcalzada/results/test","csv")

}

