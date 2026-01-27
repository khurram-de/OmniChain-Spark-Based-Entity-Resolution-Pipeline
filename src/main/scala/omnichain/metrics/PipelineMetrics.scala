package omnichain.metrics

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession, DataFrame}
import omnichain.model.Transaction
import com.ibm.icu.impl.locale.LikelySubtags.Data

object PipelineMetrics {
  def countAndPrint[T](
      ds: Dataset[T],
      dsName: String
  ): Long = {
    val count = ds.count()
    println(s"$dsName count: $count")
    count
  }

  def topBlockSizes[T](
      label: String,
      blocked: DataFrame,
      topN: Int
  ): DataFrame = {
    val spark = blocked.sparkSession
    import spark.implicits._

    val resultDF =
      blocked
        .select($"blockKey")
        .groupBy($"blockKey")
        .agg(count("*").as("count"))
        .orderBy($"count".desc)
        .limit(topN)
        .select($"blockKey", $"count")

    println(s"Top $topN block sizes for $label")
    resultDF
  }

  def blockCapHitRate(
      label: String,
      blockedBeforeCap: Dataset[(String, Transaction)],
      blockedAfterCap: Dataset[(String, Transaction)]
  ): Unit = {

    val debugExplain = false
    val spark = blockedBeforeCap.sparkSession
    import spark.implicits._

    // val countBeforeCount = blockedBeforeCap
    //   .toDF("blockKey", "tx")
    //   .groupBy($"blockKey")
    //   .count()
    //   .withColumnRenamed("count", "beforeCount")

    // val countAfterBlock = blockedAfterCap
    //   .toDF("blockKey", "tx")
    //   .groupBy($"blockKey")
    //   .count()
    //   .withColumnRenamed("count", "afterCount")

    // val joined =
    //   countBeforeCount
    //     .join(countAfterBlock, Seq("blockKey"), "left")
    //     .na
    //     .fill(0L, Seq("afterCount"))
    // if (debugExplain) {
    //   println(s"[EXPLAIN] $label - countBeforeCount")
    //   countBeforeCount.explain()
    //   println("------------------------------------------------------------")

    //   println(s"[EXPLAIN] $label - countAfterBlock")
    //   countAfterBlock.explain()
    //   println("------------------------------------------------------------")

    //   println(s"[EXPLAIN] $label - joined")
    //   joined.explain()
    //   println("------------------------------------------------------------")
    // }
    // val totalBlocksCount = countBeforeCount.count()
    // val affectedBlocksCount =
    //   joined.filter($"beforeCount" > $"afterCount").count()

    // val hitRate =
    //   if (totalBlocksCount == 0) 0.0
    //   else (affectedBlocksCount / totalBlocksCount) * 100.0

    // above is was replaced with below for performance

    val blocksBeforeDF = blockedBeforeCap
      .toDF("blockKey", "tx")
      .select($"blockKey")
      .distinct()
      .count()

    val blocksAfterDF = blockedAfterCap
      .toDF("blockKey", "tx")
      .select($"blockKey")
      .distinct()
      .count()

    // if (debugExplain) {
    //   println(s"[EXPLAIN] $label - blocksBeforeDF (groupBy blockKey)")
    //   blocksBeforeDF.explain(false)
    //   println("------------------------------------------------------------")

    //   println(s"[EXPLAIN] $label - blocksAfterDF (groupBy blockKey)")
    //   blocksAfterDF.explain(false)
    //   println("------------------------------------------------------------")
    // }

    val totalBlocksCount = blocksBeforeDF
    val affectedBlocksCount = blocksBeforeDF - blocksAfterDF

    val hitRate =
      if (totalBlocksCount == 0) 0.0
      else (affectedBlocksCount / totalBlocksCount) * 100.0

    println(
      f"$label cap hit-rate: ($affectedBlocksCount ) / $totalBlocksCount blocks affected ($hitRate%.6f%%)"
    )

    // hitRate.toLong

  }

}
