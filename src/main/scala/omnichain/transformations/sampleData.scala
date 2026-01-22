package omnichain.transformations

import omnichain.model.Transaction
import org.apache.spark.sql.{Dataset, SparkSession}

object SampleData {

  // Dummy constants for sample data (facts only)
  private val DefaultTxType: String = "TRANSFER"
  private val DefaultEventTime: Long = 0L

  /** Helper constructor to keep sample rows concise */
  private def tx(
      txId: String,
      name: String,
      wallet: String,
      amount: Double,
      txType: String = DefaultTxType,
      eventTime: Long = DefaultEventTime
  ): Transaction =
    Transaction(
      txId = txId,
      name = name,
      wallet = wallet,
      amount = amount,
      txType = txType,
      eventTime = eventTime
    )

  /** Sample transaction data with edge cases for ER testing:
    *   - Duplicate names with different wallets
    *   - Similar names / typos / casing
    *   - Same wallet for different names
    *   - Varied amounts (including decimals)
    *   - Total of 31 records
    */
  def sampleTransactions(implicit spark: SparkSession): Dataset[Transaction] = {
    import spark.implicits._

    Seq(
      tx("tx001", "John Wood Jr.", "WL0001", 272.0, eventTime = 1L),
      tx("tx002", "Alice Johnson", "WL0002", 250.5, eventTime = 1L),
      tx("tx003", "Bob Smith", "WL0003", 75.25, eventTime = 1L),
      tx("tx004", "Catherine Zeta", "WL0004", 300.0, eventTime = 1L),
      tx("tx005", "David Brown", "WL0005", 150.750, eventTime = 1L),
      tx("tx006", "Eva Green", "WL0006", 225.3, eventTime = 1L),
      tx("tx007", "Frank Miller", "WL0007", 180.9, eventTime = 1L),
      tx("tx008", "Grace Lee", "WL0008", 350.25, eventTime = 1L),
      tx("tx009", "Henry Davis", "WL0009", 125.6, eventTime = 1L),
      tx("tx010", "Ivy Chen", "WL0010", 425.1, eventTime = 1L),
      tx("tx011", "Jack Wilson", "WL0011", 272.0, eventTime = 1L),

      // Duplicate-ish name with different wallet
      tx("tx012", "Alice J.", "WL0012", 400.8, eventTime = 2L),
      tx("tx013", "Liam Scott", "WL0013", 50.0, eventTime = 2L),
      tx("tx014", "Mia Harris", "WL0014", 600.4, eventTime = 2L),
      tx("tx015", "Noah Clark", "WL0015", 320.2, eventTime = 2L),
      tx("tx016", "Olivia Lewis", "WL0016", 210.9, eventTime = 2L),

      // Similar name to test uniqueness
      tx("tx017", "Grace Li", "WL0017", 130.75, eventTime = 2L),
      tx("tx018", "Quinn Young", "WL0018", 272.0, eventTime = 3L),

      // Similar name variant
      tx("tx019", "A. Johnson", "WL0019", 290.6, eventTime = 3L),

      // Same wallet as Bob Smith to test wallet grouping
      tx("tx020", "Sam Wright", "WL0003", 360.3, eventTime = 3L),
      tx("tx021", "Tina Martinez", "WL0020", 410.0, eventTime = 3L),
      tx("tx022", "Uma Patel", "WL0021", 272.0, eventTime = 3L),
      tx("tx023", "Tim Martinez", "WL0022", 275.5, eventTime = 4L),

      // Duplicate name to test handling
      tx("tx024", "Henry Davis", "WL0023", 125.6, eventTime = 4L),

      // Typo variant
      tx("tx025", "B0b Smith", "WL0024", 500.0, eventTime = 4L),

      // Weird punctuation/spacing
      tx("tx026", "Frank  Miller%", "WL0025", 330.3, eventTime = 4L),

      // Token swap
      tx("tx027", "Clark Noah", "WL0026", 150.0, eventTime = 5L),
      tx("tx028", "David B.", "WL0027", 275.75, eventTime = 5L),
      tx("tx029", "Eva G.", "WL0028", 225.3, eventTime = 5L),
      tx("tx030", "D. Brown", "WL0029", 210.9, eventTime = 5L),

      // Casing variant
      tx("tx031", "sAm wRight", "WL0030", 180.0, eventTime = 6L)
    ).toDS()
  }

}
