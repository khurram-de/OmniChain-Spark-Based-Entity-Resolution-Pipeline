package omnichain.metrics

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession, DataFrame}
import omnichain.model.Transaction

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
      blocked: Dataset[(String, Transaction)],
      topN: Int
  ): Dataset[(String, Long)] = {
    val spark = blocked.sparkSession
    import spark.implicits._

    val resultDF =
      blocked
        .toDF("blockKey", "tx")
        .groupBy($"blockKey")
        .count()
        .orderBy($"count".desc)
        .limit(topN)
        .select($"blockKey".as[String], $"count".as[Long])
        .as[(String, Long)]

    println(s"Top $topN block sizes for $label")
    resultDF
  }

  def blockCapHitRate(
      label: String,
      blockedBeforeCap: Dataset[(String, Transaction)],
      blockedAfterCap: Dataset[(String, Transaction)]
  ): Unit = {
    val spark = blockedBeforeCap.sparkSession
    import spark.implicits._
    val countBeforeCount = blockedBeforeCap
      .toDF("blockKey", "tx")
      .groupBy($"blockKey")
      .count()
      .withColumnRenamed("count", "beforeCount")

    val countAfterBlock = blockedAfterCap
      .toDF("blockKey", "tx")
      .groupBy($"blockKey")
      .count()
      .withColumnRenamed("count", "afterCount")

    val joined =
      countBeforeCount
        .join(countAfterBlock, Seq("blockKey"), "left")
        .na
        .fill(0L, Seq("afterCount"))

    val totalBlocksCount = countBeforeCount.count()
    val affectedBlocksCount =
      joined.filter($"beforeCount" > $"afterCount").count()

    val hitRate =
      if (totalBlocksCount == 0) 0.0
      else (affectedBlocksCount / totalBlocksCount) * 100.0

    println(
      f"$label cap hit-rate: $affectedBlocksCount / $totalBlocksCount blocks affected ($hitRate%.2f%%)"
    )

  }

}
