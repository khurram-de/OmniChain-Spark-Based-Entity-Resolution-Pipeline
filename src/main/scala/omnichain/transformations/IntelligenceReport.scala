package omnichain.transformations
import org.apache.spark.sql.{Dataset, DataFrame}
import omnichain.model.Transaction
import org.apache.spark.sql.functions._

object IntelligenceReport {
  def topEntities(
      transactions: Dataset[Transaction],
      resolvedMapping: DataFrame,
      topN: Int
  ): DataFrame = {
    import transactions.sparkSession.implicits._
    transactions
      .join(resolvedMapping, Seq("txId"), "inner")
      .groupBy($"resolvedEntityId")
      .agg(
        sum($"amount").as("totalAmount"),
        count(lit(1)).as("numberOfTxns"),
        countDistinct($"wallet").as("distinctIdsMerged")
      )
      .withColumn("totalAmountFmt", format_number($"totalAmount", 0))
      .select(
        $"resolvedEntityId",
        $"totalAmount",
        $"totalAmountFmt",
        $"numberOfTxns",
        $"distinctIdsMerged"
      )
      .orderBy(
        $"totalAmount".desc,
        $"distinctIdsMerged".desc,
        $"numberOfTxns".desc
      )
      .limit(topN)
  }

}
