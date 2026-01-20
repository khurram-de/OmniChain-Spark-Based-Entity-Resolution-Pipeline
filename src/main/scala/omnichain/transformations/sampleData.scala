package omnichain.transformations
import omnichain.model.Transaction
import org.apache.spark.sql.{Dataset, SparkSession}

object SampleData {
  def sampleTransactions(spark: SparkSession): Dataset[Transaction] = {
    import spark.implicits._
    /*
     * Sample transaction data with various edge cases for testing
     * - Duplicate names with different wallets
     * - Similar names to test uniqueness handling
     * - Same wallet for different names to test grouping
     * - Varied amounts including decimals
     * - Diverse txIds to ensure uniqueness
     * - Total of 31 records
     */
    Seq(
      Transaction("tx001", "John Wood Jr.", "WL0001", 272.0),
      Transaction("tx002", "Alice Johnson", "WL0002", 250.5),
      Transaction("tx003", "Bob Smith", "WL0003", 75.25),
      Transaction("tx004", "Catherine Zeta", "WL0004", 300.0),
      Transaction("tx005", "David Brown", "WL0005", 150.750),
      Transaction("tx006", "Eva Green", "WL0006", 225.3),
      Transaction("tx007", "Frank Miller", "WL0007", 180.9),
      Transaction("tx008", "Grace Lee", "WL0008", 350.25),
      Transaction("tx009", "Henry Davis", "WL0009", 125.6),
      Transaction("tx010", "Ivy Chen", "WL0010", 425.1),
      Transaction("tx011", "Jack Wilson", "WL0011", 272.0),
      Transaction(
        "tx012",
        "Alice J.",
        "WL0012",
        400.8
      ), // Duplicate name with different wallet
      Transaction("tx013", "Liam Scott", "WL0013", 50.0),
      Transaction("tx014", "Mia Harris", "WL0014", 600.4),
      Transaction("tx015", "Noah Clark", "WL0015", 320.2),
      Transaction("tx016", "Olivia Lewis", "WL0016", 210.9),
      Transaction(
        "tx017",
        "Grace Li",
        "WL0017",
        130.75
      ), // Similar name to test uniqueness
      Transaction("tx018", "Quinn Young", "WL0018", 272.0),
      Transaction(
        "tx019",
        "A. Johnson",
        "WL0019",
        290.6
      ), // Similar name to test uniqueness
      Transaction(
        "tx020",
        "Sam Wright",
        "WL0003",
        360.3
      ), // Same wallet as Bob Smith to test wallet grouping
      Transaction(
        "tx021",
        "Tina Martinez",
        "WL0020",
        410.0
      ), // Similar name to test uniqueness
      Transaction("tx022", "Uma Patel", "WL0021", 272.0),
      Transaction("tx023", "Tim Martinez", "WL0022", 275.5),
      Transaction(
        "tx024",
        "Henry Davis",
        "WL0023",
        125.6
      ), // Duplicate name to test handling
      Transaction(
        "tx025",
        "B0b Smith",
        "WL0024",
        500.0
      ), // Similar name to test uniqueness
      Transaction(
        "tx026",
        "Frank  Miller%",
        "WL0025",
        330.3
      ), // Similar name to test uniqueness
      Transaction(
        "tx027",
        "Clark Noah",
        "WL0026",
        150.0
      ), // Similar name to test uniqueness
      Transaction("tx028", "David B.", "WL0027", 275.75),
      Transaction("tx029", "Eva G.", "WL0028", 225.3),
      Transaction(
        "tx030",
        "D. Brown",
        "WL0029",
        210.9
      ), // Similar name to test uniqueness
      Transaction(
        "tx031",
        "sAm wRight",
        "WL0030",
        180.0
      ) // Similar name to test uniqueness
    ).toDS()
  }

}
