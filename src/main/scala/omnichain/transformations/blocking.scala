package omnichain.transformations
import omnichain.model.Transaction
import org.apache.spark.sql.{SparkSession, Dataset, DataFrame}
import org.apache.spark.sql.functions._

object Blocking {
  // Placeholder for blocking transformation functions
  def normalizeName(raw: String): String = {
    /*
     * Normalizing the Names with below rules
     * - Convert to lowercase
     * - Remove special characters
     * - Replace multiple spaces with single space
     * - Replace numeric '0' with alphabetic 'O'
     * - Trim leading and trailing spaces
     */
    raw
      .toLowerCase()
      .replaceAll("[^a-zA-Z0-9\\s]", "")
      .replaceAll("\\s+", " ")
      .replaceAll("0", "O")
      .trim
  }

  def blockingKey(name: String): String = {
    /*
     * Generating blocking key by taking first 3 characters of normalized name
     * - If name length is less than 3, taking the entire normalized name
     * - Removing non-alphabetic characters
     * */
    val normalized = normalizeName(name).replaceAll("[^a-z]", "")
    if (normalized.length >= 3) normalized.take(3)
    else normalized
  }

  def withBlockingKeys(
      ds: Dataset[Transaction]
  ): Dataset[(String, Transaction)] = {
    /*
     * Adding blocking keys to each transaction record
     * - Blocking key is derived from the name field using the blockingKey function
     */
    import ds.sparkSession.implicits._
    ds.map { tx => (blockingKey(tx.name), tx) }
  }

  def blockingWithExactNameKey(
      ds: Dataset[Transaction]
  ): Dataset[(String, Transaction)] = {
    import ds.sparkSession.implicits._
    // ds.map(txn => (txn.name.trim.toLowerCase, txn))
    ds.select(
      lower(trim(col("name"))).as("_1"),
      struct(col("*")).as("_2")
    ).as[(String, Transaction)]
  }

  def topBlockSizes(
      label: String,
      blocked: DataFrame,
      topN: Int
  ): DataFrame = {
    import blocked.sparkSession.implicits._
    blocked
      .select($"blockKey") // Only select what you need BEFORE caching
      .groupBy($"blockKey")
      .agg(count("*").as("count"))
      .orderBy($"count".desc)
      .limit(topN)
      .select($"blockKey", $"count")
  }

  def blockingWithAmountCentsKey(
      ds: Dataset[Transaction]
  ): Dataset[(String, Transaction)] = {
    import ds.sparkSession.implicits._

    // ds.map { txn =>
    //   val cents = math.round(txn.amount * 100.0)
    //   // (s"amt:$cents", txn)
    //   (s"amt:${cents}|type:${txn.txType}", txn)
    // }

    // NOTE: We intentionally avoid `ds.map { txn => ... }` here.
    //
    // Why?
    // 1) `map` on a Dataset forces Spark to cross the JVM boundary and execute Scala code row-by-row.
    //    That shows up in the physical plan as:
    //      - MapElements ... scala.Tuple2
    //      - DeserializeToObject ... Transaction
    //    i.e., Spark must materialize each row as a Scala `Transaction` object and then wrap it again
    //    into a Tuple2. This adds object allocation + GC pressure and can become a bottleneck on
    //    large datasets.
    //
    // 2) Expression-based transformations (select/concat/round/lit) stay in Spark SQL / Catalyst.
    //    Catalyst can optimize the plan (projection pruning, codegen, better execution) and Spark
    //    can avoid unnecessary (de)serialization to JVM objects.
    //
    // 3) Stability at scale: when workloads grow, JVM object churn from `map` often correlates with
    //    slower jobs and a higher chance of memory issues. Using column expressions keeps the logic
    //    in Tungsten’s optimized internal format longer.
    //
    // Result: building the blocking key using Spark functions is typically faster, more memory
    // efficient, and more "Spark-native" than using `map`.

    ds.select(
      concat(
        lit("amt:"),
        (round($"amount" * 100.0, 0).cast("long")).cast("string"),
        lit("|type:"),
        $"txType"
      ),
      struct(col("*")).as("_2")
    ).as[(String, Transaction)]

  }

  def capBlocks(
      blocked: Dataset[(String, Transaction)],
      maxBlockSize: Long
  ): Dataset[(String, Transaction)] = {
    /*
     * Placeholder for blocking cap logic to limit the number of transactions per block key
     */
    /*
     * Problem: JVM Out of memory issues:
     * SortMergeJoin [blockKey#98], [blockKey#100], Inner
     * Need to refactor the code to avoid this issue
     */
    import blocked.sparkSession.implicits._

    val blockSize = blocked
      .groupByKey(_._1)
      .count()
      .toDF("blockKey", "blockSize")

    val allowedBlockKeys = blockSize
      .filter($"blockSize" <= maxBlockSize)
      .select("blockKey")

    blocked
      .toDF("blockKey", "transaction")
      .join(allowedBlockKeys.toDF("blockKey"), "blockKey", "inner")
      .as[(String, Transaction)]

  }

}
