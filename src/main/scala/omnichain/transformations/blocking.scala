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

}
