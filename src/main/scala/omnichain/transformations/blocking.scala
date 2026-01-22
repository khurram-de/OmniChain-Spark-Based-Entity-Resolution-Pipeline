package omnichain.transformations
import omnichain.model.Transaction
import org.apache.spark.sql.{SparkSession, Dataset}

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
    ds.map(txn => (txn.name.trim.toLowerCase, txn))
  }

  def blockingWithAmountCentsKey(
      ds: Dataset[Transaction]
  ): Dataset[(String, Transaction)] = {
    import ds.sparkSession.implicits._
    ds.map { txn =>
      val cents = math.round(txn.amount * 100.0)
      // (s"amt:$cents", txn)
      (s"amt:${cents}|type:${txn.txType}", txn)

    }
  }

  def capBlocks(
      blocked: Dataset[(String, Transaction)],
      maxBlockSize: Long
  ): Dataset[(String, Transaction)] = {
    /*
     * Placeholder for blocking cap logic to limit the number of transactions per block key
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
